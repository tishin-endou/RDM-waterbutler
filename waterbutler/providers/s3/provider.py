import asyncio
import hashlib
import logging

from http import HTTPStatus
from urllib.parse import unquote
import aiohttp
import botocore.exceptions
import xmltodict
import xml.sax.saxutils
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session  # type: ignore

from waterbutler.providers.s3 import settings
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import make_disposition
from waterbutler.core import streams, provider, exceptions
from waterbutler.providers.s3.metadata import (S3Revision,
                                               S3FileMetadata,
                                               S3FolderMetadata,
                                               S3FolderKeyMetadata,
                                               S3FileMetadataHeaders,
                                               )

logger = logging.getLogger(__name__)


# GRDM (K-4 / 決定-13): the S3 error codes that prove CompleteMultipartUpload did not
# assemble anything.  Everything else -- including every code absent from this table, and
# the case where no code could be read at all -- leaves the commit's outcome UNKNOWN.
#
# The asymmetry is deliberate.  Over-reporting "it may have completed" costs the user a look
# at the file list.  Under-reporting it tells the user nothing was stored, so they upload
# again: the object is now on the storage twice, counted twice against their quota, and only
# an administrator can undo that.  A hand-maintained table will eventually be out of date,
# and it has to be out of date in the direction that stays recoverable.
#
# Provenance: transcribed from the MinIO measurements in
# ``S3CompatSigv4-quota-handling/NOTE_SEMANTICS_DESIGN.md`` v2.2 §2-2, by way of PR #98.
# **Not verified against AWS S3** -- only ``EntityTooSmall`` was actually observed on a
# CompleteMultipartUpload (MinIO returned it and the object was absent afterwards); the
# other eight rest on the S3 specification and on MinIO's own error definitions.  TEST_SPEC
# E-1 reconciles the table against AWS S3.
#
# ``NoSuchUpload`` is *not* here: a second commit meets a consumed ``UploadId`` and gets that
# answer even when the first one succeeded, so it is the opposite of definitive.
DEFINITIVE_REJECTION_CODES = frozenset({
    'AccessDenied',           # no permission, so the commit never started
    'InvalidPart',            # the part set does not add up; nothing to assemble
    'InvalidPartOrder',       # likewise, out of order
    'EntityTooSmall',         # a non-final part is under the minimum
    'EntityTooLarge',         # over the size limit; the storage refused it
    'MalformedXML',           # the commit body was unreadable
    'SignatureDoesNotMatch',  # rejected at signature verification
    'InvalidAccessKeyId',     # likewise, at authentication
    'NoSuchBucket',           # there is nowhere for the object to exist
})

# The failure happened after the commit request went out, so the object may exist on the
# storage even though the upload is being reported as failed.
_COMMIT_OUTCOME_UNKNOWN_FLAG = '_wb_commit_outcome_unknown'

# The S3 error code read out of a response body that WaterButler itself parsed.  Used where
# the provider builds the exception rather than ``exception_from_response`` -- a 200 carrying
# an ``<Error>`` body, whose code would otherwise only survive inside a prose message.
_OBSERVED_ERROR_CODE_FLAG = '_wb_observed_error_code'


def _mark_commit_outcome_unknown(err):
    setattr(err, _COMMIT_OUTCOME_UNKNOWN_FLAG, True)
    return err


def _is_commit_outcome_unknown(err):
    return getattr(err, _COMMIT_OUTCOME_UNKNOWN_FLAG, False)


def _mark_observed_error_code(err, error_code):
    setattr(err, _OBSERVED_ERROR_CODE_FLAG, error_code)
    return err


class S3Provider(provider.BaseProvider):
    """Provider for Amazon's S3 cloud storage service.

    API docs: http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html

    Quirks:

    * On S3, folders are not first-class objects, but are instead inferred
      from the names of their children.  A regular DELETE request issued
      against a folder will not work unless that folder is completely empty.
      To fully delete an occupied folder, we must delete all of the comprising
      objects.  Amazon provides a bulk delete operation to simplify this.

    * A GET prefix query against a non-existent path returns 200
    """

    NAME = 's3'
    ACCEPTS_FILE_SIZE_FOR_INTRA = True
    CHUNK_SIZE = settings.CHUNK_SIZE
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
    FILE_SIZE_INTRA_COPY_LIMIT = settings.FILE_SIZE_INTRA_COPY_LIMIT

    # GRDM (K-4): appended to an upload failure when the commit's outcome is UNKNOWN.  The
    # wording is PR #98's, unchanged, so that the two providers say the same thing.
    UPLOAD_MAY_HAVE_COMPLETED_MESSAGE = (
        '  The upload may in fact have completed; please check the file list before '
        'uploading the file again.'
    )

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        .. note::

            Neither `S3Connection#__init__` nor `S3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings, **kwargs)

        self.aws_secret_access_key = credentials['secret_key']
        self.aws_access_key_id = credentials['access_key']
        self.bucket_name = settings['bucket']
        self.base_folder = self._get_base_folder(self.settings)
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.region = None

    @staticmethod
    def _get_base_folder(provider_settings):
        _, separator, base_folder = (provider_settings.get('id') or ':/').partition(':/')
        return base_folder if separator else ''

    @staticmethod
    def _error_code_of(error_element):
        """GRDM (K-4): the ``<Code>`` of a parsed S3 ``<Error>``, or ``None``.

        Case is not folded and nothing is matched as a substring.  S3 error codes are
        identifiers that agree between vendors down to the case, and a substring match would
        let ``XAccessDeniedFoo`` pass for ``AccessDenied``.
        """
        code = error_element.get('Code')
        if not isinstance(code, str):
            return None
        return code.strip() or None

    @classmethod
    def _error_code_from_body(cls, body):
        """GRDM (K-4): the S3 error code in ``body``, or ``None`` when it cannot be read.

        A body that does not parse, or parses to something that is not an ``<Error>``, is no
        code at all.  The storage said *something* went wrong but not what, which is UNKNOWN.
        """
        if not body:
            return None
        try:
            doc = xmltodict.parse(body)
        except Exception:
            return None
        error = doc.get('Error')
        if not isinstance(error, dict):
            return None
        return cls._error_code_of(error)

    @classmethod
    def _observed_error_code(cls, err):
        """GRDM (K-4): the S3 error code WaterButler actually *saw*, or ``None``.

        Only a response body may speak for the storage.  That gate is the point of this
        helper: a dropped connection carries an aiohttp message of its own, and without the
        gate an exception whose message happened to contain S3-looking XML would be
        classified as if the storage had answered.  A disconnect is exactly the case where
        nothing was observed.

        Three sources, in order:

        1. a code the provider parsed out of a body itself -- see
           :data:`_OBSERVED_ERROR_CODE_FLAG`;
        2. botocore's ``ClientError``, which carries the code in ``response['Error']``;
        3. ``exception_from_response``'s ``data``, which is a ``dict`` only when a response
           body was actually read.  Anything else -- a plain string message, a connection
           error with no ``data`` at all -- yields ``None``.
        """
        explicit = getattr(err, _OBSERVED_ERROR_CODE_FLAG, None)
        if explicit is not None:
            return explicit

        if isinstance(err, botocore.exceptions.ClientError):
            response = getattr(err, 'response', None)
            if not isinstance(response, dict):
                return None
            return response.get('Error', {}).get('Code') or None

        data = getattr(err, 'data', None)
        if not isinstance(data, dict):
            return None
        return cls._error_code_from_body(data.get('response'))

    @classmethod
    def _commit_outcome(cls, error_code):
        """GRDM (K-4): whether ``error_code`` proves the commit did not happen.

        ``None`` -- no code, or none that could be read -- is UNKNOWN, as is any code
        outside :data:`DEFINITIVE_REJECTION_CODES`.

        The two outcomes are named NOT_COMMITTED and UNKNOWN.  The ``bool`` here is those two
        names spelled ``True`` and ``False``; it stays a ``bool`` because the only caller
        uses it as a condition, and a string would have to be compared against a constant
        that a typo could silently defeat.
        """
        return error_code is not None and error_code in DEFINITIVE_REJECTION_CODES

    @classmethod
    def _commit_outcome_note(cls, err):
        """GRDM (K-4): the notice to append when the commit's outcome is genuinely unknown.

        The decision is made from the storage's error code alone.  The HTTP status class is
        deliberately *not* consulted: S3 sends the status line before it starts assembling
        the parts, so a failed CompleteMultipartUpload arrives as **200** with an ``<Error>``
        body, and the 502 this provider substitutes for it says "server error" about a
        response the storage was quite definite about.

        This is sound only because the commit is sent exactly once (決定-12, in
        ``_complete_multipart_upload``).  Under a re-send the observed code belongs to the
        *last* attempt, and a first attempt that succeeded comes back ``NoSuchUpload`` --
        at which point classifying by code says nothing about the upload.

        PR #98 suppresses the notice for quota exhaustion ahead of the table, because "you
        are out of space" and "it may have completed" contradict each other.  That branch is
        **not** ported: K-11 established that this provider has no quota mechanism at all --
        ``s3`` is in neither ``settings.ADDON_METHOD_PROVIDER`` nor ``website/util/quota.py``'s
        ``PROVIDERS`` -- so there is no quota response here to suppress.
        """
        if not _is_commit_outcome_unknown(err):
            return ''
        if cls._commit_outcome(cls._observed_error_code(err)):
            return ''
        return cls.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    @staticmethod
    def _raise_from_client_error(exc, context, error_class, code=None):
        """GRDM: re-raise ``exc`` as ``error_class``, naming only what is safe to name.

        A failure is reported as the exception's type name plus, where S3 supplied one, its
        error code.  Neither of the messages that come with these exceptions is copied over:

        * botocore's ``ClientError`` message quotes S3's own prose, which carries the request
          id and the host id.
        * everything raised out of ``make_request`` is built by
          :func:`waterbutler.core.exceptions.exception_from_response`, whose default message is
          the request URL.  Under SigV4 that URL is a presigned one, so it carries
          ``X-Amz-Credential`` -- which contains the access key id -- and ``X-Amz-Signature``.
          ``waterbutler.server.api.v1.core.write_error`` hands ``exc.message`` to the client.

        :param Exception exc: what was caught at the call site
        :param str context: the path, or the operation, the failure belongs to
        :param error_class: the WaterButler exception to raise instead
        :param int code: the status to report; ``None`` takes S3's own
        :raises: ``error_class``, or ``exc`` unchanged when it is a cancellation
        """
        if isinstance(exc, asyncio.CancelledError):
            # Python 3.6 derives CancelledError from Exception, so the broad excepts that guard
            # these calls catch it.  A cancelled request is not a provider failure: it has to
            # keep unwinding or the task it belongs to never actually stops.
            raise exc

        response = getattr(exc, 'response', None)
        if isinstance(response, dict):
            description = '{} {}'.format(
                type(exc).__name__, response.get('Error', {}).get('Code') or 'unknown')
            status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if not isinstance(status, int) or status < 400:
                # S3 answers some operations with 200 and an <Error> body when they fail part
                # way through.  botocore rewrites the response's status code to 500 so that the
                # call raises, but leaves the original 200 in ResponseMetadata.  Passing that on
                # would report the operation as having succeeded.
                status = 500
        elif isinstance(exc, exceptions.WaterButlerError):
            description = '{} {}'.format(type(exc).__name__, exc.code)
            status = exc.code
        else:
            # A transport failure -- EndpointConnectionError, TimeoutError, aiohttp's client
            # errors -- has no status of its own.
            description = type(exc).__name__
            status = None

        raise error_class('{}: {}'.format(context, description), code=code or status or 500)

    async def generate_generic_presigned_url(self, path, method='head_object', query_parameters=None, default_params=True):
        try:
            session = get_session()
            region_name = {'region_name': self.region} if self.region else {}
            endpoint_url = {'endpoint_url': f'https://s3.{self.region}.amazonaws.com'} if self.region else {'endpoint_url': 'https://s3.amazonaws.com'}
            config = AioConfig(signature_version='s3v4')

            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    config=config,
                    **region_name,
                    **endpoint_url
            ) as s3_client:
                params = {'Bucket': self.bucket_name, 'Key': path} if default_params else {}
                if query_parameters:
                    params.update(query_parameters)
                resp = await s3_client.generate_presigned_url(method, Params=params, ExpiresIn=settings.TEMP_URL_SECS)
                return resp
        except Exception as exc:
            # The status stays 404 whatever S3 said, because `BaseProvider.exists` reads a
            # NotFoundError of any status as "no", and every caller of this method goes through
            # it.  Reporting the real status is a separate change.
            self._raise_from_client_error(exc, path, exceptions.NotFoundError,
                                          code=HTTPStatus.NOT_FOUND)

    async def check_key_existence(self, path, expects=(200, ), query_parameters=None):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            endpoint_url = {'endpoint_url': f'https://s3.{self.region}.amazonaws.com'} if self.region else {'endpoint_url': 'https://s3.amazonaws.com'}
            config = AioConfig(signature_version='s3v4')
            query_parameters = query_parameters or {}

            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    config=config,
                    **region_name,
                    **endpoint_url
            ) as s3_client:
                params = {'Bucket': self.bucket_name, 'Key': path}
                if query_parameters:
                    params.update(query_parameters)

                url = await s3_client.generate_presigned_url('head_object', Params=params, ExpiresIn=settings.TEMP_URL_SECS)

                return await self.make_request(
                    'HEAD',
                    url,
                    expects=expects,
                    throws=exceptions.MetadataError,
                )
        except Exception as e:
            # See `generate_generic_presigned_url` for why the status stays 404.
            self._raise_from_client_error(e, path, exceptions.NotFoundError,
                                          code=HTTPStatus.NOT_FOUND)

    async def get_s3_bucket_object_location(self):
        try:
            session = get_session()
            config = AioConfig(signature_version='s3v4')
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    config=config
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#
                url = await s3_client.generate_presigned_url('get_bucket_location', Params={'Bucket': self.bucket_name}, ExpiresIn=settings.TEMP_URL_SECS)
                resp = await self.make_request(
                    'GET',
                    url,
                    expects=(200, ),
                    throws=exceptions.MetadataError,
                )
                return resp
        except Exception as e:
            # GRDM: this is the first request of every operation, so an unconverted botocore
            # error here surfaces as a bare 500 with nothing in it the caller can act on.
            self._raise_from_client_error(e, 'GetBucketLocation', exceptions.MetadataError)

    @staticmethod
    def _parse_listing(xml_body, root_element, path):
        """GRDM: read ``root_element`` out of a listing response, or fail.

        ``xmltodict`` keys elements by the name as written, so a body that spells its root
        ``<s3:ListBucketResult>`` -- or one that is not XML at all -- leaves a plain
        ``.get(root_element, {})`` answering ``{}``.  That is the same answer an empty bucket
        gives, and nothing downstream can tell the two apart: a folder would list as empty, and
        a delete would report success having found no version to remove.

        :param str xml_body: the response body
        :param str root_element: the element the listing is expected to be wrapped in
        :param str path: the prefix being listed, used for error messages only
        :rtype: dict
        :raises: :class:`.DownloadError` if the body does not carry ``root_element``
        """
        try:
            doc = xmltodict.parse(xml_body)
        except Exception:
            doc = {}

        result = doc.get(root_element)
        if not isinstance(result, dict):
            raise exceptions.DownloadError(
                'Could not read a {} out of the listing of {}'.format(root_element, path),
                code=HTTPStatus.BAD_GATEWAY
            )

        return result

    async def get_folder_metadata(self, path, params, next_token=None):
        """List the keys and common prefixes under ``params['Prefix']``.

        :param str path: the prefix being listed, used for error messages only
        :param dict params: the ListObjectsV2 query parameters
        :param str next_token: GRDM: when not ``None``, return a single page starting at this
            continuation token (``''`` for the first page) instead of draining the listing.
            ``None`` keeps the default behaviour of returning everything.
        :return: ``(contents, prefixes, continuation_token)``.  The token is the one to ask for
            the next page with, or ``''`` when there is no next page.
        """
        contents, response_contents, response_prefixes = [], [], []
        continuation_token = None

        # GRDM: the file browser pages through a folder, so one page has to be a bounded
        # request the UI can resume from.  Everyone else -- BaseProvider._folder_file_op,
        # BaseProvider.zip, ZipStreamGenerator -- wants the whole listing in one call.
        single_page = next_token is not None
        if single_page:
            params['MaxKeys'] = '1000'
            if next_token:
                params['ContinuationToken'] = next_token

        while True:
            if continuation_token:
                params['ContinuationToken'] = continuation_token

            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_paginator.html
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html#list-objects-v2
            list_url = await self.generate_generic_presigned_url(
                '', 'list_objects_v2', query_parameters=params, default_params=False
            )

            resp = await self.make_request(
                'GET', list_url,
                expects=(200, 206),
                throws=exceptions.DownloadError
            )
            xml_body = await resp.text()
            result = self._parse_listing(xml_body, 'ListBucketResult', path)

            contents = result.get('Contents') or []
            common_prefixes = result.get('CommonPrefixes') or []

            if isinstance(contents, dict):
                contents = [contents]
            if isinstance(common_prefixes, dict):
                common_prefixes = [common_prefixes]

            for content in contents:
                key = content.get('Key')
                if key:
                    # cast xml string encoding to display the name user downloaded (to be it compatable with make_requests),
                    # have tried yarl and furl but not see it to be helpful
                    # Todo: maybe there is a better approach (not confident all encoding is casted)
                    key = key.replace('+', ' ')
                    content['Key'] = unquote(key)
                    response_contents.append(content)

            for common_prefix in common_prefixes:
                prefix = common_prefix.get('Prefix')
                if prefix:
                    prefix = prefix.replace('+', ' ')
                    common_prefix['Prefix'] = unquote(prefix)
                    response_prefixes.append(common_prefix)

            # handle pagination
            if result.get('IsTruncated') == 'true':
                continuation_token = result.get('NextContinuationToken')
            else:
                continuation_token = None
                break

            if single_page:
                break

        return response_contents, response_prefixes, continuation_token or ''

    async def delete_objects_in_chunks(self, path, delete_requests):
        """Send ``delete_requests`` to DeleteObjects in batches of 1000, the API maximum.

        :param str path: the path being deleted, used for error messages only
        :param list delete_requests: ``{'Key': ...}`` or ``{'Key': ..., 'VersionId': ...}`` dicts
        :raises: :class:`.DeleteError` if any object in any batch was not deleted
        """
        if not delete_requests:
            # DeleteObjects rejects an empty object list.
            return

        session = get_session()
        region_name = {"region_name": self.region} if self.region else {}
        endpoint_url = {'endpoint_url': f'https://s3.{self.region}.amazonaws.com'} if self.region else {'endpoint_url': 'https://s3.amazonaws.com'}
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name,
                **endpoint_url
        ) as s3_client:
            for index in range(0, len(delete_requests), 1000):
                chunk = delete_requests[index:index + 1000]
                try:
                    result = await s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        # GRDM: Quiet=False so that per-object failures are reported back.
                        Delete={"Objects": chunk, "Quiet": False}
                    )
                except Exception as e:
                    self._raise_from_client_error(e, path, exceptions.DeleteError)

                # GRDM: DeleteObjects answers 200 even when individual objects were refused.
                # Fail closed, and name the survivors so the caller can retry them.
                errors = (result or {}).get('Errors') or []
                if errors:
                    survivors = ', '.join(
                        '{}({}) {}'.format(error.get('Key'),
                                           error.get('VersionId') or 'null',
                                           error.get('Code'))
                        for error in errors
                    )
                    raise exceptions.DeleteError(
                        'Failed to delete {} of {} objects under {}: {}'.format(
                            len(errors), len(chunk), path, survivors)
                    )

    async def get_object_versions(self, query_parameters, include_delete_markers=False):
        """List every version of the keys matched by ``query_parameters``.

        :param dict query_parameters: ListObjectVersions parameters, e.g. ``Prefix``
        :param bool include_delete_markers: also return the ``DeleteMarker`` entries.  Off by
            default so that :func:`revisions` keeps returning real revisions only; a delete
            marker is not something a user can restore or download.
        :rtype: list of dict
        """
        query_parameters = dict(query_parameters)
        query_parameters.setdefault('Bucket', self.bucket_name)

        versions_result = []
        while True:

            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html
            list_url = await self.generate_generic_presigned_url(
                '', 'list_object_versions', query_parameters=query_parameters, default_params=False
            )

            resp = await self.make_request(
                'GET', list_url,
                expects=(200, 206),
                throws=exceptions.DownloadError
            )
            xml_body = await resp.text()
            result = self._parse_listing(xml_body, 'ListVersionsResult',
                                         query_parameters.get('Prefix', ''))

            element_names = ['Version', 'DeleteMarker'] if include_delete_markers else ['Version']
            for element_name in element_names:
                entries = result.get(element_name) or []

                if isinstance(entries, dict):
                    entries = [entries]

                for entry in entries:
                    key = entry.get('Key')
                    if key:
                        # cast xml string encoding to display the name user downloaded (to be it compatable with make_requests),
                        # have tried yarl and furl but not see it to be helpful
                        # Todo: maybe there is a better approach (not confident all encoding is casted)
                        key = key.replace('+', ' ')
                        entry['Key'] = unquote(key)
                        versions_result.append(entry)

            # handle pagination.  ListObjectVersions does not use the ListObjectsV2
            # continuation token; it resumes from the last key *and* version id reported.
            if result.get('IsTruncated') != 'true':
                break

            next_key_marker = result.get('NextKeyMarker')
            next_version_id_marker = result.get('NextVersionIdMarker')
            if not next_key_marker:
                # Truncated but no marker to resume from: repeating the request would return
                # this same page forever.  Stop rather than loop.
                break

            query_parameters['KeyMarker'] = next_key_marker
            if next_version_id_marker:
                query_parameters['VersionIdMarker'] = next_version_id_marker
            else:
                query_parameters.pop('VersionIdMarker', None)

        return versions_result

    async def validate_v1_path(self, path, **kwargs):
        await self._check_region()

        path = f"/{self.base_folder + path.lstrip('/')}"

        implicit_folder = path.endswith('/')

        if implicit_folder:

            query_parameters = {'Bucket': self.bucket_name, 'Prefix': path, 'Delimiter': '/', 'MaxKeys': 1}

            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
            url = await self.generate_generic_presigned_url(path, method='list_objects_v2',
                                                            query_parameters=query_parameters, default_params=False)
            await self.make_request(
                'GET',
                url,
                expects=(200, 206,),
                throws=exceptions.NotFoundError,
            )
        else:
            await self.check_key_existence(path[1:], expects=(200, ))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        # The user selected base folder, the root of the where that user's node is connected.
        return WaterButlerPath(f"/{self.base_folder + path.lstrip('/')}")

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None, file_size=None):
        if file_size is None or file_size > self.FILE_SIZE_INTRA_COPY_LIMIT:
            return False
        return type(self) == type(dest_provider) and not path.is_dir

    def can_intra_move(self, dest_provider, path=None, file_size=None):
        if file_size is None or file_size > self.FILE_SIZE_INTRA_COPY_LIMIT:
            return False
        return type(self) == type(dest_provider) and not path.is_dir

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        await self._check_region()
        exists = await dest_provider.exists(dest_path)
        region_name = {"region_name": self.region} if self.region else {}
        endpoint_url = {'endpoint_url': f'https://s3.{self.region}.amazonaws.com'} if self.region else {'endpoint_url': 'https://s3.amazonaws.com'}

        session = get_session()
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name,
                **endpoint_url
        ) as s3_client:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_path.path,
            }
            try:
                await s3_client.copy_object(
                    Bucket=dest_provider.bucket_name,
                    Key=dest_path.path,
                    CopySource=copy_source,
                )
            except botocore.exceptions.ClientError as e:
                # GRDM (I-2): report the failure without quoting S3's own message, which carries
                # request ids, arns and bucket names, and keep the provider's status code
                # instead of flattening everything to a 500.
                self._raise_from_client_error(e, 'CopyObject failed', exceptions.IntraCopyError)

        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path, accept_url=False, revision=None, range=None, **kwargs):
        r"""Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        await self._check_region()

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        query_parameters = {}

        # Todo: don't see where it may be set from front end side
        if not revision or revision.lower() == 'latest':
            query_parameters = {}
        else:
            query_parameters['VersionId'] = revision

        display_name = kwargs.get('display_name') or path.name
        query_parameters['ResponseContentDisposition'] = make_disposition(display_name)

        url = await self.generate_generic_presigned_url(path.path, 'get_object', query_parameters=query_parameters)

        resp = await self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206,),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3
        :param str path: The full path of the key to upload to/into
        :rtype: dict, bool
        """

        await self._check_region()

        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        if stream.size < self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            await self._contiguous_upload(stream, path)
        else:
            await self._chunked_upload(stream, path)

        return (await self.metadata(path, **kwargs)), not exists

    async def _contiguous_upload(self, stream, path):
        """Uploads the given stream in one request.
        """

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}
        query_parameters = {}
        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'
            query_parameters['ServerSideEncryption'] = 'AES256'

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/put_object.html
        upload_url = await self.generate_generic_presigned_url(path.path, method='put_object', query_parameters=query_parameters)

        resp = await self.make_request(
            'PUT',
            upload_url,
            data=stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )
        await resp.release()

        # md5 is returned as ETag header as long as server side encryption is not used.
        if stream.writers['md5'].hexdigest != resp.headers['ETag'].replace('"', ''):
            raise exceptions.UploadChecksumMismatchError()

    async def _chunked_upload(self, stream, path):
        """Uploads the given stream to S3 over multiple chunks
        """
        # Step 1. Create a multi-part upload session
        session_upload_id = await self._create_upload_session(path)
        try:
            # Step 2. Break stream into chunks and upload them one by one
            parts_metadata = await self._upload_parts(stream, path, session_upload_id)
            # Step 3. Commit the parts and end the upload session
            await self._complete_multipart_upload(path, session_upload_id, parts_metadata)
        except asyncio.CancelledError:
            # GRDM: Python 3.6 derives CancelledError from Exception, so the handler below
            # catches it.  Aborting the session and reporting an upload error would stop the
            # cancellation from propagating, and the task it belongs to would never end.
            raise
        except Exception as err:
            msg = 'An unexpected error has occurred during the multi-part upload.'
            # GRDM: name the error by type and status only.  Everything raised out of
            # `make_request` reprs to the request URL, which under SigV4 is a presigned URL
            # carrying `X-Amz-Credential` -- the access key id -- and `X-Amz-Signature`.
            logger.error('{} upload_id={} error={} {}'.format(
                msg, session_upload_id, type(err).__name__, getattr(err, 'code', '')))
            # GRDM (K-4): whether the object is on the storage, and whether rubbish was left
            # behind, are two different questions.  The notice goes between the failure
            # sentence and the abort outcome so that both reach the user.
            note = self._commit_outcome_note(err)
            aborted = await self._abort_chunked_upload(path, session_upload_id)
            if not aborted:
                abort_message = '  The abort action failed to clean up the temporary file ' \
                                'parts generated during the upload process.  Please ' \
                                'manually remove them.'
            else:
                abort_message = ' The upload is aborted.'
            raise exceptions.UploadError('{}{}{}'.format(msg, note, abort_message))

    async def _create_upload_session(self, path):
        """This operation initiates a multipart upload and returns an upload ID. This upload ID is
        used to associate all of the parts in the specific multipart upload. You specify this upload
        ID in each of your subsequent upload part requests (see Upload Part). You also include this
        upload ID in the final request to either complete or abort the multipart upload request.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
        """

        headers = {}
        kwargs = {}
        # "Initiate Multipart Upload" supports AWS server-side encryption
        if self.encrypt_uploads:
            headers = {'x-amz-server-side-encryption': 'AES256'}
            kwargs["ServerSideEncryption"] = "AES256"

        # Docs: # https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/create_multipart_upload.html
        upload_session_url = await self.generate_generic_presigned_url(path.path, method='create_multipart_upload', query_parameters=kwargs)
        resp = await self.make_request(
            'POST',
            upload_session_url,
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            throws=exceptions.UploadError,
        )
        upload_session_metadata = await resp.read()
        session_data = xmltodict.parse(upload_session_metadata, strip_whitespace=False)
        # Session upload id is the only info we need
        return session_data['InitiateMultipartUploadResult']['UploadId']

    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one.
        """
        logger.error('_upload_parts')
        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.info(f'Multipart upload segment sizes: {parts}')

        for chunk_number, chunk_size in enumerate(parts):
            metadata.append(await self._upload_part(stream, path, session_upload_id,
                                                    chunk_number + 1, chunk_size))

        return metadata

    async def _upload_part(self, stream, path, session_upload_id, chunk_number, chunk_size):
        """Uploads a single part/chunk of the given stream to S3.

        :param int chunk_number: sequence number of chunk. 1-indexed.
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/upload_part.html
        upload_part_url = await self.generate_generic_presigned_url(
            path.path, method='upload_part',
            query_parameters={'ContentLength': chunk_size, 'PartNumber': chunk_number, 'UploadId': session_upload_id}
        )

        resp = await self.make_request(
            'PUT',
            upload_part_url,
            data=cutoff_stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers={'Content-Length': str(chunk_size)},
            params={'partNumber': str(chunk_number), 'uploadId': session_upload_id},
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )

        await resp.release()
        return resp.headers

    async def _abort_chunked_upload(self, path, session_upload_id):
        """This operation aborts a multipart upload. After a multipart upload is aborted, no
        additional parts can be uploaded using that upload ID. The storage consumed by any
        previously uploaded parts will be freed. However, if any part uploads are currently in
        progress, those part uploads might or might not succeed. As a result, it might be necessary
        to abort a given multipart upload multiple times in order to completely free all storage
        consumed by all parts. To verify that all parts have been removed, so you don't get charged
        for the part storage, you should call the List Parts operation and ensure the parts list is
        empty.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html

        Quirks:

        If the ABORT request is successful, the session may be deleted when the LIST PARTS request
        is made.  The criteria for successful abort thus is ether LIST PARTS request returns 404 or
        returns 200 with an empty parts list.
        """

        headers = {}
        params = {'UploadId': session_upload_id}

        abort_url = await self.generate_generic_presigned_url(path.path, method='abort_multipart_upload', query_parameters=params)

        iteration_count = 0
        is_aborted = False

        while iteration_count <= settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES:

            # ABORT
            resp = await self.make_request(
                'DELETE',
                abort_url,
                skip_auto_headers={'CONTENT-TYPE'},
                headers=headers,
                params=headers,
                expects=(204,),
                throws=exceptions.UploadError,
            )

            await resp.release()

            # LIST PARTS
            resp_xml, session_deleted = await self._list_uploaded_chunks(path, session_upload_id)

            if session_deleted:
                # Abort is successful if the session has been deleted
                is_aborted = True
                break

            uploaded_chunks_list = xmltodict.parse(resp_xml, strip_whitespace=False)
            parsed_parts_list = uploaded_chunks_list['ListPartsResult'].get('Part', [])
            if len(parsed_parts_list) == 0:
                # Abort is successful when there is no part left
                is_aborted = True
                break

            iteration_count += 1

        if is_aborted:
            logger.debug('Multi-part upload has been successfully aborted: retries={} '
                         'upload_id={}'.format(iteration_count, session_upload_id))
            return True

        logger.error('Multi-part upload has failed to abort: retries={} '
                     'upload_id={}'.format(iteration_count, session_upload_id))
        return False

    async def _list_uploaded_chunks(self, path, session_upload_id):
        """This operation lists the parts that have been uploaded for a specific multipart upload.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
        """

        headers = {}
        params = {'UploadId': session_upload_id}
        list_url = await self.generate_generic_presigned_url(path.path, method='list_parts', query_parameters=params)

        resp = await self.make_request(
            'GET',
            list_url,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            params=headers,
            expects=(200, 201, 404,),
            throws=exceptions.UploadError
        )
        session_deleted = resp.status == 404
        resp_xml = await resp.read()

        return resp_xml, session_deleted

    async def _complete_multipart_upload(self, path, session_upload_id, parts_metadata):

        """This operation completes a multipart upload by assembling previously uploaded parts.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        """

        payload = ''.join([
            '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>',
            ''.join(
                ['<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>'.format(
                    i + 1,
                    xml.sax.saxutils.escape(part['ETAG'])
                ) for i, part in enumerate(parts_metadata)]
            ),
            '</CompleteMultipartUpload>',
        ]).encode('utf-8')

        complete_url = await self.generate_generic_presigned_url(
            path.path, method='complete_multipart_upload', query_parameters={'UploadId': session_upload_id}
        )

        # GRDM (K-4): everything from here on belongs to a commit that has been sent.  Every
        # exception that escapes gets marked, whatever its type -- NOTE_SEMANTICS_DESIGN
        # v2.2 §3-3 draws the "sent" line at entering this ``await``, and narrowing the
        # ``except`` to the exception types WaterButler recognises drops the notice on the
        # rest.  A connection failure after entering the await may in fact never have put
        # anything on the wire; that is not distinguishable here, so it goes to UNKNOWN,
        # which is the recoverable side.
        try:
            resp = await self.make_request(
                'POST',
                complete_url,
                data=payload,
                headers={
                    'Content-Type': 'application/xml',
                    'Content-Length': str(len(payload)),
                },
                expects=(200, 201,),
                throws=exceptions.UploadError,
                # GRDM: the commit is sent exactly once.  CompleteMultipartUpload is not
                # idempotent -- a re-send after the first attempt succeeded meets a consumed
                # UploadId and comes back `NoSuchUpload`, so the code that reaches the caller
                # belongs to the last attempt and says nothing about the upload.  Two
                # different mechanisms re-send it and each needs its own stop: `retry=0` for
                # core's loop in `make_request` (`retry_on` covers 408/502/503/504),
                # `allow_redirects=False` for aiohttp following a 307/308 below that loop.
                # Both are scoped to this request; the part transfers and the session
                # creation keep the defaults.
                retry=0,
                allow_redirects=False,
            )
        except Exception as err:
            _mark_commit_outcome_unknown(err)
            raise

        # GRDM: S3 sends the status line before it starts assembling the parts, so a failure
        # part way through arrives as 200 with an <Error> body.  `expects` only looks at the
        # status, so without reading the body a failed commit reads as a completed upload.
        try:
            body = await resp.read()
            await resp.release()
        except Exception as err:
            # GRDM (K-4): the request went out and the storage may well have acted on it.
            # Failing to read the answer says nothing about what the answer was.
            _mark_commit_outcome_unknown(err)
            raise

        try:
            parsed = xmltodict.parse(body)
        except Exception:
            parsed = {}

        error = parsed.get('Error')
        if isinstance(error, dict):
            # GRDM (K-4): carry the parsed code on the exception.  This is the one place the
            # provider reads a code itself, so it is the one place `exception_from_response`'s
            # `data` is not there to hold it -- and recovering it from the prose below would
            # be a substring match on a message that also names the status.
            error_code = self._error_code_of(error)
            raise _mark_commit_outcome_unknown(_mark_observed_error_code(
                exceptions.UploadError(
                    'CompleteMultipartUpload answered {} with an error: {}'.format(
                        resp.status, error_code or 'unknown'),
                    code=HTTPStatus.BAD_GATEWAY
                ),
                error_code,
            ))

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        await self._check_region()

        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            # GRDM: purge every version of the key rather than issuing a plain DELETE.  On a
            # versioned bucket a plain DELETE only writes a new delete marker, leaving all the
            # previous versions -- and the storage they occupy -- behind.
            await self._delete_file_versions(path)
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_file_versions(self, path):
        """GRDM: delete every version and delete marker of a single key.

        :param *ProviderPath path: the file to purge
        :raises: :class:`.DeleteError` if the versions cannot be listed or not all of them
            could be deleted
        """
        try:
            versions = await self.get_object_versions({'Prefix': path.path},
                                                      include_delete_markers=True)
        except exceptions.WaterButlerError as exc:
            # Report the failure, not the provider's raw error document.
            raise exceptions.DeleteError(
                'Failed to list the versions of {}: {}'.format(path.path, type(exc).__name__),
                code=exc.code
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise exceptions.DeleteError(
                'Failed to list the versions of {}: {}'.format(path.path, type(exc).__name__)
            )

        # ``Prefix`` is a prefix match, so a listing for 'foo' also returns 'foo.bak'.
        delete_requests = [
            {'Key': version['Key'], 'VersionId': version['VersionId']}
            for version in versions
            if version.get('Key') == path.path and version.get('VersionId')
        ]

        await self.delete_objects_in_chunks(path.path, delete_requests)

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self._check_region

        :param *ProviderPath path: Path to be deleted
        :raises: :class:`.NotFoundError` if nothing at all is stored under the prefix

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        # docs https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/delete_objects.html#delete-objects

        GRDM: every version and delete marker under the prefix has to go, not just the live
        keys.  On a versioned bucket a listing of live keys misses both the superseded
        versions and the keys that are already delete-marked, so deleting a folder that way
        leaves its whole history -- and the storage it occupies -- behind.
        """
        await self._check_region()

        versions = await self.get_object_versions({'Prefix': path.path},
                                                  include_delete_markers=True)

        # Neither a version nor a delete marker under the prefix: the folder does not exist.
        # An empty folder is not this case -- S3 stores it as a 0-byte 'prefix/' key, which is
        # one version of its own.
        if not versions:
            raise exceptions.NotFoundError(str(path))

        delete_requests = [
            {'Key': version['Key'], 'VersionId': version['VersionId']}
            for version in versions
            if version.get('Key') and version.get('VersionId')
        ]

        await self.delete_objects_in_chunks(path.path, delete_requests)

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/list_object_versions.html
        """
        await self._check_region()

        query_params = {'Prefix': path.path, 'Delimiter': '/'}

        versions = await self.get_object_versions(query_params)

        return [
            S3Revision(item)
            for item in versions
            if item['Key'] == path.path
        ]

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        await self._check_region()

        if path.is_dir:
            # GRDM: only the API layer asks for a page at a time, and it always passes
            # `next_token` (None for the first page).  A caller that does not name the keyword
            # -- `BaseProvider._folder_file_op`, `BaseProvider.zip`, `ZipStreamGenerator` --
            # gets the complete listing, because those read `.name` off every element and a
            # continuation token among them would raise part way through a copy or a download.
            if 'next_token' in kwargs:
                metadata = await self._metadata_folder(path, next_token=kwargs['next_token'] or '')
            else:
                metadata = await self._metadata_folder(path)
            for item in metadata:
                if isinstance(item, str):
                    # the trailing continuation token, which has no `raw`
                    continue
                item.raw['base_folder'] = self.base_folder
        else:
            metadata = await self._metadata_file(path, revision=revision)
            metadata.raw['base_folder'] = self.base_folder

        return metadata

    def handle_data(self, data):
        """GRDM: split the continuation token off a paged folder listing.

        ``server.api.v1.provider.metadata`` calls this with whatever ``metadata()`` returned,
        which is either a single file's metadata or a listing that may end with a token.

        :return: ``(data, token)``, with ``token`` empty when the listing is complete
        """
        token = None
        if isinstance(data, list) and data and isinstance(data[-1], str):
            token = data.pop()

        return data, token or ''

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        await self._check_region()

        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)
        path_prefix = path.path

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/put_object.html
        folder_url = await self.generate_generic_presigned_url(path_prefix, method='put_object')

        await self.make_request(
            'PUT',
            folder_url,
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201,),
            throws=exceptions.CreateFolderError
        )

        metadata = S3FolderMetadata({'Prefix': path_prefix})
        metadata.raw['base_folder'] = self.base_folder
        return metadata

    async def _metadata_file(self, path, revision=None):
        await self._check_region()

        if revision == 'Latest':
            revision = None
        path_prefix = path.path

        resp = await self.check_key_existence(path_prefix, query_parameters={'VersionId': revision} if revision else {})
        await resp.release()
        return S3FileMetadataHeaders(path.path, resp.headers)

    async def _metadata_folder(self, path, next_token=None):
        await self._check_region()

        path_prefix = path.path
        params = {'Prefix': path_prefix, 'Delimiter': '/', 'Bucket': self.bucket_name}

        contents, prefixes, continuation_token = await self.get_folder_metadata(
            path_prefix, params, next_token=next_token)

        if not contents and not prefixes and not path.is_root:
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists
            await self.check_key_existence(path_prefix)

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            S3FolderMetadata(item)
            for item in prefixes if item['Prefix'] != path_prefix
        ]

        for content in contents:
            if content['Key'] == params['Prefix']:
                continue

            if content['Key'].endswith('/'):
                items.append(S3FolderKeyMetadata(content))
            else:
                items.append(S3FileMetadata(content))

        # GRDM: the continuation token rides along as the last element so that a single
        # metadata response can carry both a page and the cursor for the next one.
        # `handle_data` splits it back off before the listing reaches the API layer.
        if continuation_token:
            items.append(continuation_token)

        return items

    async def _check_region(self):
        """
        Lookup the region via bucket name, then update the host to match.
        """
        if self.region is None:
            self.region = await self._get_bucket_region()
            if self.region == 'EU':
                self.region = 'eu-west-1'

        self.metrics.add('region', self.region)

    async def _get_bucket_region(self):
        """Bucket names are unique across all regions.

        Endpoint doc:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html
        """
        resp = await self.get_s3_bucket_object_location()
        contents = await resp.read()
        parsed = xmltodict.parse(contents, strip_whitespace=False)
        return parsed['LocationConstraint'].get('#text', '')
