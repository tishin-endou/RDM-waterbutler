import asyncio
import hashlib
import functools
from http import HTTPStatus
from urllib import parse
import re
import logging
import xml.sax.saxutils
from xml.parsers.expat import ExpatError
from io import BytesIO
import base64

import aiohttp
import xmltodict
import boto3
from botocore.config import Config

from waterbutler.core import streams, provider, exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import make_disposition
from waterbutler.providers.s3compatsigv4 import settings
from waterbutler.providers.s3compatsigv4.metadata import (
    S3CompatSigV4Revision,
    S3CompatSigV4FileMetadata,
    S3CompatSigV4FolderMetadata,
    S3CompatSigV4FolderKeyMetadata,
    S3CompatSigV4FileMetadataHeaders,
)

logger = logging.getLogger(__name__)

# Matches an ``<Error>`` element with or without a namespace prefix, so that
# namespace-prefixed error documents are not rejected by the cheap pre-filter.
ERROR_ELEMENT_RE = re.compile(r'<(?:[^\s:>/]+:)?Error[\s>/]')

# Failures that mean "the exchange with the storage was cut short", as opposed
# to "the storage answered with an error".  ``asyncio.TimeoutError`` is NOT a
# subclass of ``aiohttp.ClientError``: the whole-request timeout that
# ``make_request`` applies (``settings.AIOHTTP_TIMEOUT``, 3600s by default)
# raises it directly, so it has to be listed explicitly.  This is the exact
# path taken when a storage stops reading the request body on quota exhaustion
# and then goes silent.
CONNECTION_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError)


def _local_name_lookup(mapping, local_name, default=None):
    """Look up ``local_name`` in an ``xmltodict`` mapping, ignoring any XML
    namespace prefix on the keys.  Returns ``default`` when absent.
    """
    if local_name in mapping:
        return mapping[local_name]
    for key, value in mapping.items():
        if isinstance(key, str) and key.rsplit(':', 1)[-1] == local_name:
            return value
    return default


def compute_md5(fp):
    """Compute MD5 hash for file-like object."""
    m = hashlib.md5()
    data = fp.read()
    m.update(data)
    fp.seek(0)
    return m.digest(), base64.b64encode(m.digest()).decode('utf-8')


class S3CompatSigV4Connection:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 endpoint_url=None, region_name=None, use_ssl=True,
                 verify_ssl=True, addressing_style='auto'):
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl

        config = Config(
            signature_version='s3v4',
            s3={
                'addressing_style': addressing_style  # 'path', 'virtual', or 'auto'
            }
        )

        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=config,
            use_ssl=use_ssl,
            verify=verify_ssl,
        )

    def generate_presigned_url(self, ClientMethod, Params=None, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod=None):
        return self.s3.meta.client.generate_presigned_url(ClientMethod, Params=Params, ExpiresIn=ExpiresIn, HttpMethod=HttpMethod)


class S3CompatSigV4Provider(provider.BaseProvider):
    """Provider for S3 Compatible Storage (SigV4) service.

    API docs: http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html

    Quirks:

    * On S3, folders are not first-class objects, but are instead inferred
      from the names of their children.  A regular DELETE request issued
      against a folder will not work unless that folder is completely empty.
      To fully delete an occupied folder, we must delete all of the comprising
      objects.  Amazon provides a bulk delete operation to simplify this.

    * A GET prefix query against a non-existent path returns 200
    """

    @property
    def NAME(self):
        return 's3compatsigv4'

    CHUNK_SIZE = settings.CHUNK_SIZE
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT

    QUOTA_EXCEEDED_MESSAGE = (
        'Upload failed because the quota or capacity of the cloud storage has been exceeded.  '
        'Please free up storage space or contact the storage administrator.'
    )
    UNCLASSIFIED_STORAGE_ERROR_MESSAGE = (
        'Upload failed because the cloud storage returned an error that could not be '
        'interpreted.  Please retry the upload, and contact the storage administrator if the '
        'problem persists.'
    )
    # A dropped connection is only evidence of exhausted capacity, never proof:
    # it is just as often a network fault.  Naming both keeps the message from
    # sending the user off to free up space when nothing is full.
    CONNECTION_INTERRUPTED_MESSAGE = (
        'Upload failed because the connection to the cloud storage was interrupted before the '
        'upload completed.  This may indicate that the storage is full, that its quota has '
        'been exceeded, or that there was a network problem.  Please retry the upload, and '
        'contact the storage administrator if the problem persists.'
    )

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key`, `secret_key`, `host`
        :param dict settings: Dict containing `bucket` and optional `region`, `prefix`
        """
        super().__init__(auth, credentials, settings, **kwargs)

        host = credentials['host']
        port = 443
        m = re.match(r'^(.+)\:([0-9]+)$', host)
        if m is not None:
            host = m.group(1)
            port = int(m.group(2))

        is_secure = (port == 443)
        protocol = 'https' if is_secure else 'http'
        if port in (80, 443):
            endpoint_url = '{}://{}'.format(protocol, host)
        else:
            endpoint_url = '{}://{}:{}'.format(protocol, host, port)

        self.bucket_name = self.settings['bucket']
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.region = self.settings.get('region', None)
        self.prefix = self.settings.get('prefix', '')

        self.connection = S3CompatSigV4Connection(
            aws_access_key_id=credentials['access_key'],
            aws_secret_access_key=credentials['secret_key'],
            endpoint_url=endpoint_url,
            region_name=self.region,
            use_ssl=is_secure,
            verify_ssl=is_secure
        )
        self.bucket = self.connection.s3.Bucket(self.bucket_name)

    async def validate_v1_path(self, path, **kwargs):
        wbpath = WaterButlerPath(path, prepend=self.prefix)
        if path == '/':
            return wbpath

        implicit_folder = path.endswith('/')

        prefix = wbpath.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'
        if implicit_folder:
            query_parameters = {
                'Bucket': self.bucket_name,
                'Prefix': prefix,
                'Delimiter': '/',
            }
            resp = await self.make_request(
                'GET',
                functools.partial(
                    self.connection.generate_presigned_url,
                    'list_objects_v2',
                    Params=query_parameters,
                    HttpMethod='GET',
                ),
                expects=(
                    HTTPStatus.OK,
                    HTTPStatus.NOT_FOUND,
                ),
                throws=exceptions.MetadataError,
            )
        else:
            query_parameters = {'Bucket': self.bucket_name, 'Key': prefix}
            resp = await self.make_request(
                'HEAD',
                functools.partial(
                    self.connection.generate_presigned_url,
                    'head_object',
                    Params=query_parameters,
                    HttpMethod='HEAD',
                ),
                expects=(
                    HTTPStatus.OK,
                    HTTPStatus.NOT_FOUND,
                ),
                throws=exceptions.MetadataError,
            )

        await resp.release()

        if resp.status == HTTPStatus.NOT_FOUND:
            raise exceptions.NotFoundError(str(prefix))

        return wbpath

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.prefix)

    def can_duplicate_names(self):
        return True

    @staticmethod
    def _check_for_200_error(
        response_body,
        s3_api_name='S3 API',
        exception_type=exceptions.UnhandledProviderError,
    ):
        """check an S3 API result with http status is 200 OK.

        try to parse response body as a xml.
        if the xml has an 'Error' element then raise an exception.

        :param str response_body: API response body.
        :param str s3_api_name: S3 API name for logging.
        :param type exception_type: raise Exception type
        """
        try:
            # memo: If no element, the parser will raise an ExpatError.
            result = xmltodict.parse(response_body)
        except ExpatError:
            logger.warning('Couldn\'t parse %s result', s3_api_name)
            raise

        if 'Error' in result:
            error_code = result['Error'].get('Code', 'Unknown')
            logger.warning('%s returned with an error: %s', s3_api_name, error_code)
            raise exception_type(
                f'{s3_api_name} returned with an error.',
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    async def download(self, path, accept_url=False, revision=None, range=None, **kwargs):
        r"""Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param path: ( :class:`.WaterButlerPath` ) Path to the key you want to download
        :param kwargs: (dict) Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=HTTPStatus.BAD_REQUEST)

        # MEMO: This is a workaround for the bug on some callers.
        if revision is None and 'version' in kwargs:
            revision = kwargs['version']

        try:
            pre_size, pre_etag = await self._get_content_whole_size(path, revision)
            if range is not None:
                # MEMO: range type is (int, int)
                # see: core/provider.py _build_range_header()
                s, e = range
                if s is None or e is None:
                    pre_size = None
                elif s < 0 or s >= pre_size or e < 0 or e >= pre_size or e < s:
                    pre_size = None
                else:
                    pre_size = e - s + 1
        except exceptions.MetadataError:
            logger.debug('Could not retrieve metadata for pre-flight check, skipping')
            pre_size = None
            pre_etag = None

        if not revision or revision.lower() == 'latest':
            query_parameters = None
        else:
            query_parameters = {'VersionId': revision}

        display_name = kwargs.get('display_name') or path.name
        response_headers = {
            'ResponseContentDisposition': make_disposition(display_name)
        }

        query_parameters_dict = {'Bucket': self.bucket_name, 'Key': path.full_path}
        if query_parameters:
            query_parameters_dict.update(query_parameters)
        query_parameters_dict.update(response_headers)

        headers = {}
        resp = await self.make_request(
            'GET',
            functools.partial(
                self.connection.generate_presigned_url,
                'get_object',
                Params=query_parameters_dict,
                HttpMethod='GET',
            ),
            range=range,
            headers=headers,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            throws=exceptions.DownloadError,
        )

        try:
            get_etag = resp.headers['ETag'].replace('"', '')
            if get_etag != pre_etag:
                pre_size = None
        except KeyError:
            # ETag header may not be present in all responses
            pass

        download_stream = streams.ResponseStreamReader(resp)

        if hasattr(download_stream, '_size') and download_stream._size is None:
            # if the GetObject API doesn't return Content-Length header,
            # use metadata content-size or range size instead of it.
            download_stream._size = pre_size

        return download_stream

    async def _get_content_whole_size(self, path: WaterButlerPath, revision=None):
        """get content whole size from path."""
        metadata = await self.metadata(path, revision)
        try:
            size = metadata.size_as_int
            etag = metadata.etag
        except KeyError:
            raise exceptions.MetadataError('Cannot get content size and ETag')
        return size, etag

    @staticmethod
    def _raw_error_body(err):
        """Return the storage's raw response body carried by ``err``, or ``None``.

        ``exception_from_response`` stores an XML error body either as
        ``err.data['response']`` (dict) or as ``err.message`` (str).
        """
        body = None
        data = getattr(err, 'data', None)
        if isinstance(data, dict):
            body = data.get('response')
        if body is None:
            body = getattr(err, 'message', None)
        return body if isinstance(body, str) else None

    @classmethod
    def _parse_s3_error_body(cls, err):
        """Extract the S3 XML error ``Code`` and ``Message`` from an
        :class:`waterbutler.core.exceptions.UploadError` raised by ``make_request``.

        :param err: ( :class:`.UploadError` ) The error raised by ``make_request``
        :rtype: tuple(str or None, str or None)
        :return: ``(error_code, error_message)``, or ``(None, None)`` when the
            response body is not a parsable S3 XML error
        """
        body = cls._raw_error_body(err)
        if body is None or not ERROR_ELEMENT_RE.search(body):
            return None, None
        try:
            parsed = xmltodict.parse(body)
        except ExpatError:
            return None, None
        if not isinstance(parsed, dict):
            return None, None
        error = _local_name_lookup(parsed, 'Error')
        if not isinstance(error, dict):
            # ``<Error>text</Error>`` parses to a plain string, and an empty
            # document parses to ``None``.  Neither carries an error code.
            return None, None
        code = _local_name_lookup(error, 'Code')
        message = _local_name_lookup(error, 'Message')
        if not isinstance(code, str) or not code.strip():
            # An empty ``<Code/>`` is ``None`` and ``<Code attr="..."/>`` is a
            # dict; without a code there is nothing to translate.
            return None, None
        return code.strip(), message.strip() if isinstance(message, str) else None

    @classmethod
    def _is_quota_exhaustion(cls, err):
        """Whether ``err`` is the storage reporting that it is out of space.

        ``_translate_upload_error`` and the ``_chunked_upload`` entry log both
        report the same failure, so they have to agree on this.  When only the
        translator knew that quota exhaustion is an expected, user-resolvable
        outcome, the entry log still went out at ERROR and paged oncall every
        time somebody filled a bucket.

        Because the list of vendor-specific quota codes cannot be exhaustive,
        a response that already carries HTTP 507 counts whatever its code is.
        """
        if not isinstance(err, exceptions.UploadError):
            return False
        if err.code == HTTPStatus.INSUFFICIENT_STORAGE:
            return True
        error_code = cls._parse_s3_error_body(err)[0]
        return error_code is not None and error_code in settings.QUOTA_EXCEEDED_ERROR_CODES

    def _translate_upload_error(self, err, extra_message=''):
        """Translate a raw :class:`.UploadError` from the storage backend into a
        user-facing error.

        Storage-side quota exhaustion (e.g. ``QuotaExceeded``) is mapped to HTTP
        507 (Insufficient Storage) with an explicit message so that the user can
        tell why and how the upload ended.  Other S3 XML errors are re-raised
        with a readable message instead of the raw XML body.

        Because the list of vendor-specific quota error codes cannot be
        exhaustive, a response that already carries HTTP 507 is treated as a
        quota failure whatever its error code is (or even without a parsable
        body).

        :param err: ( :class:`.UploadError` ) The original error
        :param str extra_message: An optional message appended to the translated
            error (e.g. a warning that the multipart-upload abort failed)
        :rtype: :class:`.UploadError`
        """
        error_code, error_message = self._parse_s3_error_body(err)
        is_quota_error = self._is_quota_exhaustion(err)

        if is_quota_error:
            code_note = '  (storage error code: {})'.format(error_code) \
                if error_code is not None else ''
            return exceptions.UploadError(
                '{}{}{}'.format(self.QUOTA_EXCEEDED_MESSAGE, code_note, extra_message),
                code=HTTPStatus.INSUFFICIENT_STORAGE,
                # Running out of storage is an expected, user-resolvable failure:
                # keep it out of Sentry's error level and the 5xx alerting path.
                is_user_error=True,
            )
        if error_code is not None:
            return exceptions.UploadError(
                'Upload failed because the cloud storage returned an error.  '
                '(storage error code: {}, message: {}){}'.format(
                    error_code, error_message, extra_message),
                code=err.code,
            )
        # Nothing could be classified.  ``err`` must still not be handed back:
        # ``BaseHandler.write_error`` writes ``exc.data`` verbatim as the
        # response body when it is set, and falls back to ``exc.message``
        # otherwise.  For a dict message that body is the storage's raw XML
        # (Resource paths, RequestId); for the string message that
        # ``exception_from_response`` builds when there is no body, it is the
        # presigned URL including its signature.
        return exceptions.UploadError(
            '{}{}'.format(self.UNCLASSIFIED_STORAGE_ERROR_MESSAGE, extra_message),
            code=err.code)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3 Compatible Storage

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3 Compatible Storage
        :param path: ( :class:`.WaterButlerPath` ) The full path of the key to upload to/into

        :rtype: dict, bool
        """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        if stream.size < self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            await self._contiguous_upload(stream, path)
        else:
            await self._chunked_upload(stream, path)

        return (await self.metadata(path, **kwargs)), not exists

    async def _contiguous_upload(self, stream, path):
        """Uploads the given stream in one request."""

        # Read the entire stream to compute MD5
        stream_data = await stream.read()
        md5_digest = hashlib.md5(stream_data).digest()
        md5_base64 = base64.b64encode(md5_digest).decode('utf-8')

        # Create a new stream from the data
        upload_stream = streams.StringStream(stream_data)

        headers = {
            'Content-Length': str(len(stream_data)),
            'Content-MD5': md5_base64,
        }

        # this is usually set in boto3 presigned url, but do it here
        # to be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        query_parameters = {'Bucket': self.bucket_name, 'Key': path.full_path}

        try:
            resp = await self.make_request(
                'PUT',
                functools.partial(
                    self.connection.generate_presigned_url,
                    'put_object',
                    Params=query_parameters,
                    HttpMethod='PUT',
                ),
                data=upload_stream,
                skip_auto_headers={'CONTENT-TYPE'},
                headers=headers,
                expects=(
                    HTTPStatus.OK,
                    HTTPStatus.CREATED,
                ),
                throws=exceptions.UploadError,
            )
        except exceptions.UploadError as err:
            # Translate storage-side errors (e.g. quota exceeded) into a
            # user-facing error instead of returning the raw XML body.
            # ``from None``: the untranslated error is what carries the raw
            # body, and a chained ``__context__`` puts it straight back into
            # the rendered traceback.
            raise self._translate_upload_error(err) from None
        except CONNECTION_ERRORS as err:
            # Some S3-compatible storages close the connection while the client
            # is still sending the request body (e.g. when the storage-side
            # quota has been exceeded).  Without this handler the raw client
            # error propagates as an unexplained HTTP 500.
            logger.error('Connection error during contiguous upload: {!r}'.format(err))
            raise exceptions.UploadError(self.CONNECTION_INTERRUPTED_MESSAGE,
                                         code=HTTPStatus.BAD_GATEWAY)

        # S3-compatible server automatically validates Content-MD5
        # If MD5 doesn't match, server returns 400 UploadError before writing data

        await resp.release()

    async def _chunked_upload(self, stream, path):
        """Uploads the given stream to S3 over multiple chunks"""

        # Step 1. Create a multi-part upload session
        try:
            session_upload_id = await self._create_upload_session(path)
        except exceptions.UploadError as err:
            # The session has not been created, so there is nothing to abort.
            # Storage-side quota errors can occur at session creation too.
            # ``from None``: the untranslated error is what carries the raw
            # body, and a chained ``__context__`` puts it straight back into
            # the rendered traceback.
            raise self._translate_upload_error(err) from None
        except CONNECTION_ERRORS as err:
            logger.error('Connection error during multipart session creation: {!r}'.format(err))
            raise exceptions.UploadError(self.CONNECTION_INTERRUPTED_MESSAGE,
                                         code=HTTPStatus.BAD_GATEWAY)

        try:
            # Step 2. Break stream into chunks and upload them one by one
            parts_metadata = await self._upload_parts(stream, path, session_upload_id)
            # Step 3. Commit the parts and end the upload session
            await self._complete_multipart_upload(path, session_upload_id, parts_metadata)
        except Exception as err:
            msg = 'An unexpected error has occurred during the multi-part upload.'
            logger.error('{} upload_id={} error={!r}'.format(msg, session_upload_id, err))
            aborted = await self._abort_chunked_upload(path, session_upload_id)
            abort_message = ''
            if not aborted:
                # NOTE: this warning must be appended only when the abort has
                # FAILED.  (An earlier revision appended it on success.)
                abort_message = '  The abort action failed to clean up the temporary file ' \
                                'parts generated during the upload process.  Please manually ' \
                                'remove them.'
            if isinstance(err, exceptions.UploadError):
                # Preserve the original storage error (status code and body) and
                # translate quota-exhaustion responses into a user-facing error.
                raise self._translate_upload_error(
                    err, extra_message=abort_message) from None
            if isinstance(err, CONNECTION_ERRORS):
                raise exceptions.UploadError(
                    '{}{}'.format(self.CONNECTION_INTERRUPTED_MESSAGE, abort_message),
                    code=HTTPStatus.BAD_GATEWAY,
                )
            raise exceptions.UploadError('{}{}'.format(msg, abort_message))

    async def _create_upload_session(self, path):
        """This operation initiates a multipart upload and returns an upload ID. This upload ID is
        used to associate all of the parts in the specific multipart upload. You specify this upload
        ID in each of your subsequent upload part requests (see Upload Part). You also include this
        upload ID in the final request to either complete or abort the multipart upload request.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
        """

        headers = {}
        # "Initiate Multipart Upload" supports AWS server-side encryption
        if self.encrypt_uploads:
            headers = {'x-amz-server-side-encryption': 'AES256'}

        query_parameters = {'Bucket': self.bucket_name, 'Key': path.full_path}

        resp = await self.make_request(
            'POST',
            functools.partial(
                self.connection.generate_presigned_url,
                'create_multipart_upload',
                Params=query_parameters,
                ExpiresIn=200,
                HttpMethod='POST',
            ),
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(
                HTTPStatus.OK,
                HTTPStatus.CREATED,
            ),
            throws=exceptions.UploadError,
        )
        upload_session_metadata = await resp.read()
        session_data = xmltodict.parse(upload_session_metadata, strip_whitespace=False)
        # Session upload id is the only info we need
        return session_data['InitiateMultipartUploadResult']['UploadId']

    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one."""

        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.debug('Multipart upload segment sizes: {}'.format(parts))
        for chunk_number, chunk_size in enumerate(parts):
            logger.debug('  uploading part {} with size {}'.format(chunk_number + 1, chunk_size))
            metadata.append(await self._upload_part(stream, path, session_upload_id,
                                                    chunk_number + 1, chunk_size))
        return metadata

    async def _upload_part(self, stream, path, session_upload_id, chunk_number, chunk_size):
        """Uploads a single part/chunk of the given stream to S3.

        :param int chunk_number: sequence number of chunk. 1-indexed.
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)

        headers = {'Content-Length': str(chunk_size)}
        query_parameters = {
            'Bucket': self.bucket_name,
            'Key': path.full_path,
            'PartNumber': chunk_number,
            'UploadId': session_upload_id,
        }

        resp = await self.make_request(
            'PUT',
            functools.partial(
                self.connection.generate_presigned_url,
                'upload_part',
                Params=query_parameters,
                ExpiresIn=200,
                HttpMethod='PUT',
            ),
            data=cutoff_stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(
                HTTPStatus.OK,
                HTTPStatus.CREATED,
            ),
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
        query_parameters = {
            'Bucket': self.bucket_name,
            'Key': path.full_path,
            'UploadId': session_upload_id,
        }

        iteration_count = 0
        is_aborted = False
        while iteration_count < settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES:
            try:
                # ABORT
                resp = await self.make_request(
                    'DELETE',
                    functools.partial(
                        self.connection.generate_presigned_url,
                        'abort_multipart_upload',
                        Params=query_parameters,
                        HttpMethod='DELETE',
                    ),
                    skip_auto_headers={'CONTENT-TYPE'},
                    headers=headers,
                    expects=(HTTPStatus.NO_CONTENT,),
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
            except Exception as err:
                msg = 'An unexpected error has occurred during the aborting a multipart upload.'
                logger.error('{} upload_id={} error={!r}'.format(msg, session_upload_id, err))

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
        query_parameters = {
            'Bucket': self.bucket_name,
            'Key': path.full_path,
            'UploadId': session_upload_id,
        }

        resp = await self.make_request(
            'GET',
            functools.partial(
                self.connection.generate_presigned_url,
                'list_parts',
                Params=query_parameters,
                HttpMethod='GET',
            ),
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(
                HTTPStatus.OK,
                HTTPStatus.CREATED,
                HTTPStatus.NOT_FOUND,
            ),
            throws=exceptions.UploadError,
        )
        session_deleted = resp.status == HTTPStatus.NOT_FOUND
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
        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }
        query_parameters = {
            'Bucket': self.bucket_name,
            'Key': path.full_path,
            'UploadId': session_upload_id
        }

        resp = await self.make_request(
            'POST',
            functools.partial(
                self.connection.generate_presigned_url,
                'complete_multipart_upload',
                Params=query_parameters,
                ExpiresIn=200,
                HttpMethod='POST',
            ),
            data=payload,
            headers=headers,
            expects=(
                HTTPStatus.OK,
                HTTPStatus.CREATED,
            ),
            throws=exceptions.UploadError,
        )

        response_body = await resp.read()
        self._check_for_200_error(response_body, "CompleteMultipartUpload", exceptions.UploadError)

        await resp.release()

    async def move(self, dest_provider, src_path, dest_path,
                  rename=None, conflict='replace', handle_naming=True):
        """Override move to clean up orphaned S3 folder prefix objects after move."""
        result = await super().move(
            dest_provider, src_path, dest_path,
            rename=rename, conflict=conflict, handle_naming=handle_naming,
        )

        # After moving a folder, clean up orphaned folder prefix object at source
        if not src_path.is_file:
            prefix = src_path.full_path.lstrip('/')
            try:
                await self._delete_folder_prefix(prefix)
            except exceptions.DeleteError:
                logger.warning('Failed to clean up folder prefix after move: %s', prefix)

        return result

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete the key and all its versions at the specified path

        :param path: ( :class:`.WaterButlerPath` ) The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=HTTPStatus.BAD_REQUEST,
                )
        logger.debug('Deleting path: %s', path.full_path)
        if path.is_file:
            # Retrieve and delete all versions (batched) similar to S3 provider
            try:
                prefix = path.full_path.lstrip('/')
                query_params = {
                    'Prefix': prefix,
                    'Delimiter': '/',
                    'VersionIdMarker': '',
                }
                _, versions, delete_markers = await self.get_full_revision(query_params)
                full_version_list = versions + delete_markers
                if full_version_list:
                    version_dict = {
                        path.full_path: [
                            v.get('VersionId')
                            for v in full_version_list
                            if v.get('VersionId')
                        ]
                    }

                    version_ids = version_dict[path.full_path]
                    # AWS allows max 1000 objects per delete_objects call
                    for i in range(0, len(version_ids), 1000):
                        batch = version_ids[i: i + 1000]
                        delete_list = [
                            {'Key': path.full_path, 'VersionId': vid} for vid in batch
                        ]
                        # Run synchronous boto3 call in executor to avoid blocking
                        loop = asyncio.get_event_loop()
                        response = await loop.run_in_executor(
                            None,
                            lambda d=delete_list: self.bucket.delete_objects(
                                Delete={'Objects': d, 'Quiet': False}
                            ),
                        )
                        # Check for errors in response
                        if 'Errors' in response and response['Errors']:
                            error_count = len(response['Errors'])
                            error_codes = [e.get('Code', 'Unknown') for e in response['Errors']]
                            logger.error(
                                'Errors deleting objects: count=%d, codes=%s', error_count, error_codes
                            )
                            raise exceptions.DeleteError(
                                'Failed to delete some objects: {} error(s)'.format(error_count)
                            )
                        deleted_count = len(response.get('Deleted', []))
                        logger.debug('Batch deleted %d versions', deleted_count)
                else:
                    # No versions -> delete current object directly
                    query_parameters = {
                        'Bucket': self.bucket_name,
                        'Key': path.full_path,
                    }
                    resp = await self.make_request(
                        'DELETE',
                        functools.partial(
                            self.connection.generate_presigned_url,
                            'delete_object',
                            Params=query_parameters,
                            HttpMethod='DELETE',
                        ),
                        expects=(
                            HTTPStatus.OK,
                            HTTPStatus.NO_CONTENT,
                        ),
                        throws=exceptions.DeleteError,
                    )
                    await resp.release()
            except exceptions.MetadataError:
                # Versions cannot be retrieved (e.g. MinIO without versioning).
                # Fall back to deleting the current version directly.
                logger.debug('Version listing not available, falling back to direct delete')
                query_parameters = {'Bucket': self.bucket_name, 'Key': path.full_path}
                resp = await self.make_request(
                    'DELETE',
                    functools.partial(
                        self.connection.generate_presigned_url,
                        'delete_object',
                        Params=query_parameters,
                        HttpMethod='DELETE',
                    ),
                    expects=(
                        HTTPStatus.OK,
                        HTTPStatus.NO_CONTENT,
                    ),
                    throws=exceptions.DeleteError,
                )
                await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder_prefix(self, prefix):
        """Delete the folder prefix object (e.g. 'foldername/') from S3."""
        query_parameters = {'Bucket': self.bucket_name, 'Key': prefix}
        resp = await self.make_request(
            'DELETE',
            functools.partial(
                self.connection.generate_presigned_url,
                'delete_object',
                Params=query_parameters,
                HttpMethod='DELETE',
            ),
            expects=(
                HTTPStatus.OK,
                HTTPStatus.NO_CONTENT,
            ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def _folder_prefix_exists(self, folder_prefix):
        # Even if the storage is MinIO, Contents with a leaf folder is
        # returned when a last slash of a prefix is removed.
        query_parameters = {
            'Bucket': self.bucket_name,
            'Prefix': folder_prefix.rstrip('/'),  # 'A/B/' -> 'A/B'
            'Delimiter': '/'
        }
        resp = await self.make_request(
            'GET',
            functools.partial(
                self.connection.generate_presigned_url,
                'list_objects_v2',
                Params=query_parameters,
                HttpMethod='GET',
            ),
            expects=(HTTPStatus.OK,),
            throws=exceptions.MetadataError,
        )
        response_body = await resp.read()
        parsed = xmltodict.parse(response_body, strip_whitespace=False)['ListBucketResult']
        common_prefixes = parsed.get('CommonPrefixes', [])
        # common_prefixes is dict when returned prefix is one.
        if not isinstance(common_prefixes, list):
            common_prefixes = [common_prefixes]
        for common_prefix in common_prefixes:
            val = common_prefix.get('Prefix')
            if val == folder_prefix:  # with last slash
                return True
        return False

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self.make_request
               func: self.connection.generate_presigned_url

        :param *ProviderPath path: Path to be deleted

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        """
        if not path.full_path.endswith('/'):
            raise exceptions.InvalidParameters('not a folder: {}'.format(str(path)))

        prefix = path.full_path.lstrip('/')
        list_query_params = {'Prefix': prefix}
        try:
            parsed, versions, delete_markers = await self.get_full_revision(dict(list_query_params))
        except exceptions.MetadataError:
            # Versioning not supported (e.g., MinIO). Fall back to ListObjectsV2.
            all_objects = []
            continuation_token = None
            while True:
                query_params = {
                    'Bucket': self.bucket_name,
                    'Prefix': prefix,
                    'MaxKeys': 1000,
                }
                if continuation_token:
                    query_params['ContinuationToken'] = continuation_token
                resp = await self.make_request(
                    'GET',
                    functools.partial(
                        self.connection.generate_presigned_url,
                        'list_objects_v2',
                        Params=query_params,
                        HttpMethod='GET',
                    ),
                    expects=(HTTPStatus.OK,),
                    throws=exceptions.MetadataError,
                )
                contents = await resp.read()
                parsed = xmltodict.parse(
                    contents.decode('utf-8'),
                    strip_whitespace=False,
                )['ListBucketResult']
                objects = parsed.get('Contents', [])
                if isinstance(objects, dict):
                    objects = [objects]
                all_objects.extend({'Key': obj['Key']} for obj in objects if obj.get('Key'))

                if parsed.get('IsTruncated') == 'true':
                    continuation_token = parsed.get('NextContinuationToken')
                else:
                    break

            if all_objects:
                for i in range(0, len(all_objects), 1000):
                    batch = all_objects[i:i + 1000]
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda d=batch: self.bucket.delete_objects(
                            Delete={'Objects': d, 'Quiet': False}
                        ),
                    )
                    if response.get('Errors'):
                        error_count = len(response['Errors'])
                        error_codes = [e.get('Code', 'Unknown') for e in response['Errors']]
                        logger.error('_delete_folder fallback: %d delete error(s), codes=%s', error_count, error_codes)
                        raise exceptions.DeleteError(
                            'Failed to delete some objects: {} error(s)'.format(error_count)
                        )
            # Also clean up folder prefix
            try:
                await self._delete_folder_prefix(prefix)
            except exceptions.DeleteError:
                logger.warning('Failed to clean up folder prefix in _delete_folder fallback: %s', prefix)
            return

        if not versions and not delete_markers:
            # No objects/versions -> treat as missing (parity with S3 provider)
            raise exceptions.NotFoundError(str(path))

        version_map = {}
        keys_without_version = []
        for item in versions + delete_markers:
            key = item.get('Key')
            if not key:
                continue
            version_id = item.get('VersionId')
            if version_id:
                version_map.setdefault(key, []).append(version_id)
            else:
                keys_without_version.append({'Key': key})

        all_objects = [
            {'Key': k, 'VersionId': v} for k, vids in version_map.items() for v in vids
        ] + keys_without_version
        # AWS allows max 1000 objects per delete_objects call
        for i in range(0, len(all_objects), 1000):
            batch = all_objects[i: i + 1000]
            # Run synchronous boto3 call in executor to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda b=batch: self.bucket.delete_objects(
                    Delete={'Objects': b, 'Quiet': False}
                ),
            )
            # Check for errors in response
            if 'Errors' in response and response['Errors']:
                error_count = len(response['Errors'])
                error_codes = [e.get('Code', 'Unknown') for e in response['Errors']]
                logger.error(
                    'Errors deleting folder objects: count=%d, codes=%s', error_count, error_codes
                )
                raise exceptions.DeleteError(
                    'Failed to delete some objects: {} error(s)'.format(error_count)
                )
            deleted_count = len(response.get('Deleted', []))
            logger.debug('Batch deleted %d objects from folder', deleted_count)

        # Clean up folder prefix object if it still exists
        if await self._folder_prefix_exists(prefix):
            await self._delete_folder_prefix(prefix)

    async def get_full_revision(self, query_params):
        """
        Get all versions and delete markers of the requested object
        :param query_params: The query parameters to be used in the request
        :return: The dict of response content, list versions and delete_markers
        """
        versions = []
        delete_markers = []
        more_to_come = True

        while more_to_come:
            query_parameters_dict = {'Bucket': self.bucket_name}
            query_parameters_dict.update(query_params)
            resp = await self.make_request(
                'GET',
                functools.partial(
                    self.connection.generate_presigned_url,
                    'list_object_versions',
                    Params=query_parameters_dict,
                    HttpMethod='GET',
                ),
                expects=(HTTPStatus.OK,),
                throws=exceptions.MetadataError,
            )

            response_body = await resp.read()
            parsed = xmltodict.parse(response_body.decode('utf-8'), strip_whitespace=False)['ListVersionsResult']

            # Append current page's versions and delete markers
            current_versions = parsed.get('Version', [])
            current_delete_markers = parsed.get('DeleteMarker', [])

            if isinstance(current_versions, dict):
                current_versions = [current_versions]
            if isinstance(current_delete_markers, dict):
                current_delete_markers = [current_delete_markers]

            # boto3 automatically adds encoding-type=url to presigned URLs
            # for list_object_versions, causing MinIO to return URL-encoded
            # keys (e.g. Japanese characters). Decode them so that
            # delete_objects receives the actual key names.
            if parsed.get('EncodingType') == 'url':
                for item in current_versions:
                    if 'Key' in item:
                        item['Key'] = parse.unquote(item['Key'])
                for item in current_delete_markers:
                    if 'Key' in item:
                        item['Key'] = parse.unquote(item['Key'])

            versions.extend(current_versions)
            delete_markers.extend(current_delete_markers)

            # Check if more pages are available
            more_to_come = parsed.get('IsTruncated') == 'true'
            if more_to_come:
                query_params['KeyMarker'] = parsed.get('NextKeyMarker')
                if parsed.get('EncodingType') == 'url' and query_params['KeyMarker']:
                    query_params['KeyMarker'] = parse.unquote(query_params['KeyMarker'])
                query_params['VersionIdMarker'] = parsed.get('NextVersionIdMarker')

        return parsed, versions, delete_markers

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param path: ( :class:`.WaterButlerPath` ) The path to a key
        :rtype list:
        """
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'
        query_parameters = {
            'Bucket': self.bucket_name,
            'Prefix': prefix,
            'Delimiter': '/'
        }
        list_url = self.connection.generate_presigned_url('list_object_versions', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='GET')
        try:
            resp = await self.make_request(
                'GET',
                list_url,
                expects=(HTTPStatus.OK,),
                throws=exceptions.MetadataError,
            )
        except exceptions.MetadataError as e:
            # MinIO may not support "versions" from boto3 presigned url.
            # (And, MinIO does not support ListObjectVersions yet.)
            logger.info('ListObjectVersions may not be supported: %s', str(e))
            return []

        response_body = await resp.read()
        xml = xmltodict.parse(response_body.decode('utf-8'))
        versions = xml['ListVersionsResult'].get('Version') or []

        if isinstance(versions, dict):
            versions = [versions]

        return [
            S3CompatSigV4Revision(item)
            for item in versions
            if item['Key'] == prefix
        ]

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            if 'next_token' in kwargs:
                return await self._metadata_folder(path, kwargs['next_token'])
            return (await self._metadata_folder(path))

        return (await self._metadata_file(path, revision=revision))

    def handle_data(self, data):
        token = None
        if not isinstance(data, S3CompatSigV4FileMetadataHeaders):
            token = data.pop()

        return data, token or ''

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param path: ( :class:`.WaterButlerPath` ) The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)

        query_parameters = {'Bucket': self.bucket_name, 'Key': path.full_path}

        async with self.request(
            'PUT',
            functools.partial(
                self.connection.generate_presigned_url,
                'put_object',
                Params=query_parameters,
                HttpMethod='PUT',
            ),
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(
                HTTPStatus.OK,
                HTTPStatus.CREATED,
            ),
            throws=exceptions.CreateFolderError,
        ):
            return S3CompatSigV4FolderMetadata(self, {'Prefix': path.full_path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        query_parameters = {'Bucket': self.bucket_name, 'Key': path.full_path}
        if revision:
            query_parameters['VersionId'] = revision

        resp = await self.make_request(
            'HEAD',
            functools.partial(
                self.connection.generate_presigned_url,
                'head_object',
                Params=query_parameters,
                HttpMethod='HEAD',
            ),
            expects=(HTTPStatus.OK,),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        return S3CompatSigV4FileMetadataHeaders(self, path.full_path, resp.headers)

    async def _metadata_folder(self, path, next_token=None):
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'
        query_parameters = {
            'Bucket': self.bucket_name,
            'Prefix': prefix,
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url',
        }
        if next_token:
            query_parameters['ContinuationToken'] = next_token

        resp = await self.make_request(
            'GET',
            functools.partial(
                self.connection.generate_presigned_url,
                'list_objects_v2',
                Params=query_parameters,
                HttpMethod='GET',
            ),
            expects=(HTTPStatus.OK,),
            throws=exceptions.MetadataError,
        )

        contents = await resp.read()
        parsed = xmltodict.parse(contents.decode('utf-8'), strip_whitespace=False)['ListBucketResult']

        next_token_string = parsed.get('NextContinuationToken', '')
        contents = parsed.get('Contents', [])
        prefixes = parsed.get('CommonPrefixes', [])

        # Decode URL-encoded fields after XML parsing (not before).
        # EncodingType=url causes S3 to URL-encode specific fields (Key, Prefix, etc.)
        # but decoding the entire XML before parsing would break XML with
        # special characters (e.g. & in key names).
        if parsed.get('EncodingType') == 'url':
            if isinstance(contents, dict):
                contents = [contents]
            for item in contents:
                if 'Key' in item:
                    item['Key'] = parse.unquote(item['Key'])
            if isinstance(prefixes, dict):
                prefixes = [prefixes]
            for item in prefixes:
                if 'Prefix' in item:
                    item['Prefix'] = parse.unquote(item['Prefix'])
            if next_token_string:
                next_token_string = parse.unquote(next_token_string)

        if not contents and not prefixes and not path.is_root:
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists
            query_parameters = {'Bucket': self.bucket_name, 'Key': prefix}
            resp = await self.make_request(
                'HEAD',
                functools.partial(
                    self.connection.generate_presigned_url,
                    'head_object',
                    Params=query_parameters,
                    HttpMethod='HEAD',
                ),
                expects=(HTTPStatus.OK,),
                throws=exceptions.MetadataError,
            )
            await resp.release()

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            S3CompatSigV4FolderMetadata(self, item)
            for item in prefixes
        ]

        for content in contents:
            if content['Key'] == path.full_path:  # self
                continue

            if content['Key'].endswith('/'):
                items.append(S3CompatSigV4FolderKeyMetadata(self, content))
            else:
                items.append(S3CompatSigV4FileMetadata(self, content))

        if next_token_string:
            items.append(next_token_string)
        return items
