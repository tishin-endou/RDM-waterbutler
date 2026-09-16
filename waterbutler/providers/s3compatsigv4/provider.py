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

# Error codes that mean the storage declined the CompleteMultipartUpload
# *before* assembling anything, so the object cannot exist.  Everything else --
# including codes that are not in this table at all, and responses whose code
# could not be observed -- is treated as UNKNOWN and gets the "it may have
# completed" notice.
#
# The fail-safe points this way on purpose.  An unnecessary notice sends the
# user to check a file list that turns out not to contain the file: annoying,
# and they recover on their own.  A missing notice sends them to upload the
# file a second time when the first one did land, which consumes the quota
# twice and needs an administrator to undo.  A table that is extended by hand
# will eventually be out of date, and it has to be out of date in the direction
# that stays recoverable.
#
# Deliberately hardcoded rather than read from ``settings``: the quota codes in
# ``settings.QUOTA_EXCEEDED_ERROR_CODES`` *are* operator-extensible, and the
# suppression rule in ``_commit_outcome_note`` depends on the quota branch
# being evaluated first precisely because the two lists cannot be kept in step.
# Making this one configurable too would reintroduce that coupling.
#
# Only ``EntityTooSmall`` has been confirmed against a real storage (MinIO
# returned it for CompleteMultipartUpload and the object was absent
# afterwards); the other eight rest on the S3 specification and MinIO's own
# error definitions.
DEFINITIVE_REJECTION_CODES = frozenset({
    'AccessDenied',         # no permission, so the commit never started
    'InvalidPart',          # the part set does not add up; nothing to assemble
    'InvalidPartOrder',     # likewise, out of order
    'EntityTooSmall',       # a non-final part is under the minimum
    'EntityTooLarge',       # over the size limit; the storage refused it
    'MalformedXML',         # the commit body was unreadable
    'SignatureDoesNotMatch',  # rejected at signature verification
    'InvalidAccessKeyId',   # likewise, at authentication
    'NoSuchBucket',         # there is nowhere for the object to exist
})

# Upper bound, **in bytes**, on how much of the storage's raw error body is
# written to the log.  The body is the only place ``RequestId`` / ``Resource``
# survive once the error has been translated into a summary message, but it must
# not be unbounded: a misconfigured proxy can answer with a full HTML page.
ERROR_BODY_LOG_LIMIT = 512


def _bounded_body(body):
    """The leading :data:`ERROR_BODY_LOG_LIMIT` **bytes** of ``body``, as text.

    Both log sites go through this so that the bound means the same thing at
    each.  ``'%.*s'`` counts *characters*, which lets a Japanese error message
    or a non-ASCII HTML error page through at up to three times the declared
    size -- the very case the constant's comment is about.

    Bodies arrive as ``bytes`` straight off the wire in one place and as ``str``
    (already decoded by ``exception_from_response``) in the other, so both are
    accepted.  Cutting bytes can split a multibyte character, hence ``replace``.

    Cutting the input to the limit is not enough on its own, as measured:
    ``replace`` substitutes U+FFFD for every byte it cannot decode, and U+FFFD
    is three bytes of UTF-8, so ``b'\\xff' * 512`` came back as 1536 bytes --
    three times the declared bound.  The result is
    therefore **re-measured** after decoding and cut again, on a character
    boundary (``ignore`` drops the partial character the second cut leaves).
    The first cut still happens, so a multi-megabyte HTML page is never decoded
    in full; it is safe because decoding never shrinks a byte string -- valid
    UTF-8 round-trips and invalid bytes expand -- so a 512-byte prefix always
    has at least 512 bytes of output to give.
    """
    if body is None:
        return None
    if isinstance(body, str):
        body = body.encode('utf-8', 'replace')
    head = body[:ERROR_BODY_LOG_LIMIT].decode('utf-8', 'replace')
    return head.encode('utf-8')[:ERROR_BODY_LOG_LIMIT].decode('utf-8', 'ignore')


# Sentinel for "the key is not in the mapping at all".  ``xmltodict`` maps an
# empty element to ``None`` (``<Error/>``, ``<Error></Error>`` and
# ``<Error>   </Error>`` all become ``{'Error': None}``), so ``None`` on its own
# cannot distinguish "absent" from "present but empty".  Those two must not be
# conflated: an empty ``<Error>`` element still means the request failed.
_MISSING = object()


# ``_translate_upload_error`` interprets an error by parsing the storage's XML
# body out of it.  That is only meaningful for errors that actually carry one.
# WaterButler also raises ``UploadError`` with its own prose -- e.g. the 502
# ``_create_upload_session`` raises when the session response is unreadable --
# and those must pass through untouched: parsing them yields "unclassifiable"
# and the fallback would overwrite the message with a generic one.
#
# The distinction is carried explicitly on the exception rather than inferred
# from its shape.  Inferring it (e.g. "``data`` is a dict, so it must be raw")
# is not safe: the two kinds are indistinguishable by inspection, so a newly
# added WaterButler-authored error would silently pick the wrong branch.
_STORAGE_RESPONSE_FLAG = '_wb_storage_response'

# The failure was observed while committing a multipart upload, so the object
# may exist on the storage even though the request is reported as failed.
_COMMIT_OUTCOME_UNKNOWN_FLAG = '_wb_commit_outcome_unknown'


def _mark_storage_response(err):
    """Tag ``err`` as carrying a raw storage response body."""
    setattr(err, _STORAGE_RESPONSE_FLAG, True)
    return err


def _is_storage_response(err):
    return getattr(err, _STORAGE_RESPONSE_FLAG, False)


def _mark_commit_outcome_unknown(err):
    setattr(err, _COMMIT_OUTCOME_UNKNOWN_FLAG, True)
    return err


def _is_commit_outcome_unknown(err):
    return getattr(err, _COMMIT_OUTCOME_UNKNOWN_FLAG, False)


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
    # "before the upload completed" was removed deliberately: the same message
    # is returned when the connection drops while *reading the answer to the
    # commit*, and there the upload may well have completed.  Asserting the
    # opposite is what sends the user off to upload the file a second time.
    CONNECTION_INTERRUPTED_MESSAGE = (
        'Upload failed because the connection to the cloud storage was interrupted.  This may '
        'indicate that the storage is full, that its quota has been exceeded, or that there '
        'was a network problem.  Please retry the upload, and contact the storage '
        'administrator if the problem persists.'
    )
    # The commit is still reported as failed (fail-closed), but an unreadable
    # answer is not proof that nothing was written.  Saying only "the upload
    # failed" invites a duplicate upload.
    UPLOAD_MAY_HAVE_COMPLETED_MESSAGE = (
        '  The upload may in fact have completed; please check the file list before '
        'uploading the file again.'
    )

    async def _make_upload_request(self, *args, **kwargs):
        """``make_request`` for the upload path, tagging storage-origin failures.

        Every ``UploadError`` that escapes here was built by
        ``exception_from_response`` from an actual storage response, so it is
        the raw material ``_translate_upload_error`` is allowed to interpret.
        Marking at the source keeps that judgement next to the request that
        justifies it, instead of re-deriving it from the exception's shape at
        the point of use.
        """
        try:
            return await self.make_request(*args, **kwargs)
        except exceptions.UploadError as err:
            _mark_storage_response(err)
            raise

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

        The raised exception carries the raw body under ``data['response']``,
        which is the same shape :func:`.exceptions.exception_from_response`
        builds for a genuine non-2xx XML response.  Keeping the two shapes
        identical is what lets ``_translate_upload_error`` recognise a quota
        failure that S3 reported with HTTP 200 (CompleteMultipartUpload does
        this) instead of surfacing a bare HTTP 500.

        The check is deliberately *fail-closed*: once an ``Error`` element is
        seen the operation has failed, so being unable to classify it (empty
        element, missing ``Code``, unparsable body) still raises.  Returning
        normally would let ``upload()`` go on to read the *previous* object's
        metadata and report a successful upload of data that was never
        committed.

        :param str response_body: API response body.
        :param str s3_api_name: S3 API name for logging.
        :param type exception_type: raise Exception type
        """
        body = response_body.decode('utf-8', 'replace') \
            if isinstance(response_body, bytes) else response_body

        try:
            # memo: If no element, the parser will raise an ExpatError.
            result = xmltodict.parse(response_body)
        except ExpatError:
            # Letting ExpatError escape surfaces as a bare HTTP 500 with a
            # stack trace: ``_translate_upload_error`` has no ``.message`` to
            # work with on it.  The storage answered unintelligibly, which is
            # an upstream fault, so report HTTP 502 -- consistently with
            # ``_create_upload_session``.
            logger.warning('Couldn\'t parse %s result', s3_api_name)
            raise _mark_storage_response(
                exception_type({'response': body}, code=HTTPStatus.BAD_GATEWAY))

        error = _local_name_lookup(result, 'Error', _MISSING) \
            if isinstance(result, dict) else _MISSING
        if error is _MISSING:
            return

        error_code = None
        if isinstance(error, dict):
            code = _local_name_lookup(error, 'Code')
            if isinstance(code, str) and code.strip():
                error_code = code.strip()
        logger.warning('%s returned with an error: %s', s3_api_name, error_code or 'Unknown')

        # The storage reported a failure inside a 2xx response.  Sending the
        # request was not something WaterButler got wrong, so the fault is
        # attributed upstream: HTTP 502 rather than 500.
        # ``_translate_upload_error`` refines this to 507 when the body turns
        # out to be a quota rejection.
        #
        # The synthesised status carries no information about the outcome, and
        # it is not asked to: ``_commit_outcome_note`` classifies on the error
        # code alone, and the code is still in ``body`` for it to read.
        raise _mark_storage_response(
            exception_type({'response': body}, code=HTTPStatus.BAD_GATEWAY))

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
        # The code-matching rule requires surrounding whitespace to be
        # removed before the code is matched.  ``xmltodict`` 0.9.0 already
        # strips text nodes, so these ``.strip()`` calls are redundant *today*
        # and deleting them changes no behaviour; they are kept rather than
        # removed because the rule must not depend silently on a third party's
        # default.
        # ``test_the_xml_parser_is_what_strips_the_code`` watches that default,
        # so a dependency bump that drops it turns these back into the only
        # thing holding the rule up instead of quietly breaking the
        # classification.
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
        if not isinstance(err, exceptions.UploadError) or not _is_storage_response(err):
            return False
        if err.code == HTTPStatus.INSUFFICIENT_STORAGE:
            return True
        error_code = cls._parse_s3_error_body(err)[0]
        return error_code is not None and error_code in settings.QUOTA_EXCEEDED_ERROR_CODES

    @classmethod
    def _observed_error_code(cls, err):
        """The S3 error code WaterButler actually *saw*, or ``None``.

        The gate on ``_is_storage_response`` is the point of this helper.
        ``_raw_error_body`` falls back to ``err.message`` when there is no
        response payload, and aiohttp's connection errors carry a message of
        their own -- so without the gate, an exception whose message happened to
        contain S3-looking XML would be classified as if the storage had
        answered.  A dropped connection is exactly the case where nothing was
        observed, and it must not be able to speak for the storage.
        """
        if not _is_storage_response(err):
            return None
        # ``_parse_s3_error_body`` already strips surrounding whitespace, which
        # covers ``<Code>\n  AccessDenied\n</Code>``.  Case is *not* folded and
        # the comparison below is exact: S3 error codes are identifiers that
        # agree between vendors down to the case, and a substring match would
        # let ``XQuotaExceededFoo`` pass for ``QuotaExceeded``.
        return cls._parse_s3_error_body(err)[0]

    @classmethod
    def _commit_outcome(cls, error_code):
        """Whether ``error_code`` proves the commit did not happen.

        ``None`` -- no code, or none that could be read -- is UNKNOWN, as is any
        code outside :data:`DEFINITIVE_REJECTION_CODES`.

        The two outcomes are named ``NOT_COMMITTED`` and ``UNKNOWN``.  The
        ``bool`` here is those two names spelled ``True`` and ``False``:
        ``True`` is NOT_COMMITTED, ``False`` is UNKNOWN.  Kept as a ``bool``
        because the only caller uses it as a condition, and a string would have
        to be compared against a constant that a typo could silently defeat.
        """
        return error_code is not None and error_code in DEFINITIVE_REJECTION_CODES

    @classmethod
    def _commit_outcome_note(cls, err):
        """The notice to append when the commit's outcome is genuinely unknown.

        The decision is made from the storage's error code alone:

        1. **Quota exhaustion suppresses the notice, and is checked first.**
           Running out of space is the storage refusing to keep the object, so
           "it may have completed" would contradict the very message it is
           appended to.  This is not a fallback to the status class -- it is a
           suppression, and it has to come first because the two lists cannot be
           kept in step: ``_is_quota_exhaustion`` counts *any* HTTP 507 whatever
           its code, and operators can extend
           ``settings.QUOTA_EXCEEDED_ERROR_CODES`` at will, so quota responses
           routinely carry codes that no hardcoded table knows.  Measured during
           the design review: 4 of 5 quota-positive inputs were absent from the
           table, and every one of them produced the self-contradictory message.
        2. **Otherwise the code decides**, via
           :data:`DEFINITIVE_REJECTION_CODES`.  Anything else is UNKNOWN.

        The HTTP status class is deliberately *not* consulted.  It cannot carry
        this: ``_check_for_200_error`` synthesises HTTP 502 for every
        200-with-``<Error>`` body, which is the shape a failed
        CompleteMultipartUpload actually takes, so the status says "5xx" for
        responses the storage was quite definite about.  The rule lives here
        rather than at each mark site so that the ways a commit can fail cannot
        drift apart.

        This is sound only because the commit is sent exactly once.  Two things
        hold that up, and they stop different re-sends: ``retry=0`` stops
        WaterButler's own retry loop, ``allow_redirects=False`` stops the HTTP
        client following a 307/308.  Both are in
        ``_complete_multipart_upload``.  Under either kind of re-send the
        observed code is the *last* attempt's, and a first attempt that
        succeeded would come back as ``NoSuchUpload`` -- at which point
        classifying by code says nothing about the upload.
        """
        if not _is_commit_outcome_unknown(err):
            return ''
        if cls._is_quota_exhaustion(err):
            return ''
        if cls._commit_outcome(cls._observed_error_code(err)):
            return ''
        return cls.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

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
        if not _is_storage_response(err):
            # WaterButler authored this message, so there is no storage body to
            # interpret and the text is already user-facing.  Parsing it would
            # yield "unclassifiable" and the fallback below would replace it
            # with a generic message -- losing, for example, the instruction to
            # have an administrator remove a stale multipart session.
            #
            # This branch is also where an upload call site that forgot
            # ``_make_upload_request`` would land, and it degrades *quietly* in
            # two ways at once.  Quota errors would stop becoming 507s -- and,
            # less visibly, ``_commit_outcome_note`` gates on the same tag, so
            # the commit notice would stop appearing too: an untagged commit
            # failure tells the user nothing was stored when nobody knows that.
            # Neither degradation fails anything.  DEBUG rather than WARNING
            # because the legitimate case is the common one -- this is a
            # breadcrumb for whoever is asking why a 507 or a notice did not
            # happen, not an alert.
            logger.debug('Passing through an untagged upload error unmodified: '
                         'type=%s status=%s', type(err).__name__, err.code)
            if not extra_message:
                return err
            return exceptions.UploadError(
                '{}{}'.format(err.message, extra_message),
                code=err.code,
                # Rebuilding the exception must not silently re-classify it.
                # ``is_user_error`` decides the Sentry level in
                # ``server/api/v1/core.py``; defaulting it to ``False`` here
                # would promote a user-caused failure to an error-level event
                # purely because the abort warning had to be appended.
                is_user_error=err.is_user_error,
            )

        error_code, error_message = self._parse_s3_error_body(err)
        is_quota_error = self._is_quota_exhaustion(err)
        outcome_note = self._commit_outcome_note(err)

        # The translated error only carries a summary message, so this is the
        # single place where the storage's own diagnostics (RequestId, Resource)
        # can still be recorded.  Both upload paths funnel through here.
        # Quota exhaustion is logged one level down: it is an expected,
        # user-resolvable failure and should not trip log-based alerting (the
        # same reasoning as ``is_user_error`` below).
        raw_body = self._raw_error_body(err)
        # ``int()`` keeps the rendering stable across Python versions: before
        # 3.11 ``'%s' % HTTPStatus.FORBIDDEN`` is ``'HTTPStatus.FORBIDDEN'``.
        status = int(err.code) if isinstance(err.code, int) else err.code
        log = logger.warning if is_quota_error else logger.error
        log('Storage rejected the upload: status=%s code=%s body=%s',
            status, error_code, _bounded_body(raw_body))

        if is_quota_error:
            code_note = '  (storage error code: {})'.format(error_code) \
                if error_code is not None else ''
            # ``outcome_note`` is appended here like everywhere else, and is
            # always empty on this path -- ``_commit_outcome_note`` suppresses
            # it for quota exhaustion, first, before consulting the code table.
            #
            # Writing it out rather than omitting it is the point.  An earlier
            # revision suppressed the notice *here* instead, which meant the
            # rule was enforced in two places and neither could be tested: a
            # mutation that deleted the quota branch from
            # ``_commit_outcome_note`` left all tests green, because this
            # omission silently covered for it.  One suppression, in one place,
            # that a test can actually reach.
            return exceptions.UploadError(
                '{}{}{}{}'.format(self.QUOTA_EXCEEDED_MESSAGE, code_note,
                                  outcome_note, extra_message),
                code=HTTPStatus.INSUFFICIENT_STORAGE,
                # Running out of storage is an expected, user-resolvable failure:
                # keep it out of Sentry's error level and the 5xx alerting path.
                is_user_error=True,
            )
        if error_code is not None:
            return exceptions.UploadError(
                'Upload failed because the cloud storage returned an error.  '
                '(storage error code: {}, message: {}){}{}'.format(
                    error_code, error_message, outcome_note, extra_message),
                code=err.code,
            )
        # Nothing could be classified.  ``err`` must still not be handed back:
        # ``BaseHandler.write_error`` writes ``exc.data`` verbatim as the
        # response body when it is set, and falls back to ``exc.message``
        # otherwise.  For a dict message that body is the storage's raw XML
        # (Resource paths, RequestId); for the string message that
        # ``exception_from_response`` builds when there is no body, it is the
        # presigned URL including its signature.  The raw body is already in
        # the log above, which is where it belongs.
        return exceptions.UploadError(
            '{}{}{}'.format(self.UNCLASSIFIED_STORAGE_ERROR_MESSAGE, outcome_note,
                            extra_message),
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
            resp = await self._make_upload_request(
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
            # Type only.  aiohttp builds ``ClientOSError(errno, 'Can not write
            # request body for <url>')`` in ``ClientRequest.write_bytes``, and
            # for this provider that URL is presigned -- its ``str()`` carries
            # ``X-Amz-Credential`` and ``X-Amz-Signature``.  A socket dying
            # mid-body is the event this handler exists for, so rendering the
            # exception here would put the signature in the log on the common
            # path, not a rare one.
            logger.error('Connection error during contiguous upload: error_type=%s',
                         type(err).__name__)
            # ``from None`` for the same reason: a chained ``__context__``
            # renders the original message into the traceback Sentry stores.
            raise exceptions.UploadError(self.CONNECTION_INTERRUPTED_MESSAGE,
                                         code=HTTPStatus.BAD_GATEWAY) from None

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
            # Type only, and ``from None``: see ``_contiguous_upload``.  The
            # presigned URL reaches this path exactly the same way.
            logger.error('Connection error during multipart session creation: error_type=%s',
                         type(err).__name__)
            raise exceptions.UploadError(self.CONNECTION_INTERRUPTED_MESSAGE,
                                         code=HTTPStatus.BAD_GATEWAY) from None

        try:
            # Step 2. Break stream into chunks and upload them one by one
            parts_metadata = await self._upload_parts(stream, path, session_upload_id)
            # Step 3. Commit the parts and end the upload session
            await self._complete_multipart_upload(path, session_upload_id, parts_metadata)
        except Exception as err:
            msg = 'An unexpected error has occurred during the multi-part upload.'
            # Type and status only.  ``repr()`` of a WaterButlerError renders
            # its whole message, and for a dict message that is the storage's
            # raw body serialised as JSON -- unbounded, and about to be logged
            # again (bounded) by ``_translate_upload_error``.
            err_code = getattr(err, 'code', None)
            # Quota exhaustion is expected and the user can resolve it, so it
            # must not go out at ERROR here after the translator has already
            # classified it as a warning -- otherwise this line alone keeps
            # paging oncall.
            log = logger.warning if self._is_quota_exhaustion(err) else logger.error
            log('%s upload_id=%s error_type=%s error_code=%s', msg, session_upload_id,
                type(err).__name__,
                int(err_code) if isinstance(err_code, int) else err_code)
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
                # The notice applies whenever the commit was the request that
                # dropped -- this is the path a read failure at commit time
                # takes.  It does not depend on the exception carrying a status:
                # ``_commit_outcome_note`` consults the S3 error *code* and
                # nothing else, so a ``ClientResponseError`` with a ``.code`` of
                # 400, 500 or 507 is treated exactly like a bare
                # ``ServerDisconnectedError`` (measured).  There is
                # deliberately no fallback to the HTTP status class.
                #
                # ``from None``: aiohttp's message for a mid-body disconnect
                # embeds the presigned URL, and a chained ``__context__`` would
                # carry it into the traceback.
                raise exceptions.UploadError(
                    '{}{}{}'.format(self.CONNECTION_INTERRUPTED_MESSAGE,
                                    self._commit_outcome_note(err), abort_message),
                    code=HTTPStatus.BAD_GATEWAY,
                ) from None
            # No ``code=``, so this one defaults to 500 -- not to 502.  The
            # ``UploadError`` branch above goes through
            # ``_translate_upload_error``, which keeps the storage's own status
            # (403 stays 403) and answers 507 for quota exhaustion; only the
            # ``CONNECTION_ERRORS`` branch is a fixed 502.
            # The 500 is still deliberate, and for a reason that does not depend
            # on what the others return: an exception that is neither an
            # ``UploadError`` (the storage answered) nor a ``CONNECTION_ERRORS``
            # member (the link failed) did not come from upstream at all -- it
            # is a defect on this side, and any upstream-attributing status
            # would blame the storage for it.  Pinned by
            # ``test_chunked_upload_unexpected_error_is_a_500``.
            raise exceptions.UploadError(
                '{}{}{}'.format(msg, self._commit_outcome_note(err),
                                abort_message)) from None

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

        resp = await self._make_upload_request(
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
        try:
            upload_session_metadata = await resp.read()
        finally:
            # A storage that is out of room tends to drop the connection
            # mid-body, and ``read()`` then raises with the connection still
            # held.  A read failure here means no ``UploadId`` was ever
            # obtained, so ``_chunked_upload`` returns from its
            # ``CONNECTION_ERRORS`` handler at step 1 and never aborts anything.
            # The reason to release is the ordinary one: the connector is
            # shared, and a leak here is paid for by every later request on this
            # provider -- the part uploads, the commit, and the abort that a
            # *later* failure does reach.
            await resp.release()
        try:
            session_data = xmltodict.parse(upload_session_metadata, strip_whitespace=False)
            # Session upload id is the only info we need
            session_upload_id = session_data['InitiateMultipartUploadResult']['UploadId']
            if not isinstance(session_upload_id, str) or not session_upload_id.strip():
                # An empty ``<UploadId/>`` parses to ``None`` and an attribute-only
                # element to a dict.  Returning either would make every following
                # request use a bogus upload id.
                raise ValueError('UploadId is missing or blank')
            # ``strip_whitespace=False`` keeps the indentation of a
            # pretty-printed body inside the element, and the id goes straight
            # into the ``uploadId`` query parameter of every following request
            # (and into its SigV4 signature).
            return session_upload_id.strip()
        except (ExpatError, KeyError, TypeError, ValueError) as err:
            # The storage returned 200/201 but the body is not the expected XML.
            # NOTE: at this point a multipart session MAY have been created on
            # the storage side but its UploadId is unknown, so it cannot be
            # aborted here.  Log enough information for manual clean-up.
            # Type only, and the body bounded by the shared constant: a second
            # hard-coded limit here would drift away from the translator's.
            logger.error('Failed to parse the CreateMultipartUpload response: key=%s '
                         'error_type=%s body=%s', path.full_path, type(err).__name__,
                         _bounded_body(upload_session_metadata))
            raise exceptions.UploadError(
                'Failed to create a multipart upload session: the cloud storage returned an '
                'unexpected response.  A stale multipart upload session may remain on the '
                'storage; please ask the storage administrator to check for and remove it.',
                code=HTTPStatus.BAD_GATEWAY,
            )

    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one."""

        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.debug('Multipart upload segment sizes: %s', parts)
        for chunk_number, chunk_size in enumerate(parts):
            logger.debug('  uploading part %s with size %s', chunk_number + 1, chunk_size)
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

        resp = await self._make_upload_request(
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

    @staticmethod
    def _log_abort_failure(err, session_upload_id, s3_error_code=None):
        """Log an abort attempt that failed, by *kind* rather than by content.

        ``'{!r}'.format(err)`` renders ``WaterButlerError.__repr__``, which
        embeds the whole message -- and for an error built from a storage
        response that message is the raw body serialised as JSON.  It is
        unbounded, it carries the storage's ``Resource`` paths, and it is
        emitted once per retry.  ``_translate_upload_error`` already logs the
        body once, bounded by ``ERROR_BODY_LOG_LIMIT``, so repeating it here
        buys nothing.  The type and status are what actually identify the
        failure when reading the log.

        Dropping the body still has to leave the log able to say *which* S3
        error this was.  The caller has already parsed the code to test for
        ``NoSuchUpload``, so passing it on costs nothing; ``error_code=``
        then means the same thing here as it does in the entry log, and the
        HTTP status gets its own name.
        """
        status = getattr(err, 'code', None)
        logger.error('An unexpected error has occurred during the aborting a multipart '
                     'upload. upload_id=%s error_type=%s status=%s error_code=%s',
                     session_upload_id, type(err).__name__,
                     int(status) if isinstance(status, int) else status, s3_error_code)

    @staticmethod
    def _no_parts_left(resp_xml):
        """Whether a LIST PARTS body reports an empty parts list."""
        # An element with no children parses to ``None``, not to an empty dict.
        result = xmltodict.parse(resp_xml, strip_whitespace=False)['ListPartsResult'] or {}
        # ``IsTruncated`` means this is one page of a longer listing, so the
        # absence of ``Part`` *here* says nothing about the pages after it.
        # This answer is what suppresses the "please remove the parts manually"
        # warning, so an incomplete listing must not be read as "nothing left".
        if str(result.get('IsTruncated', '')).strip().lower() == 'true':
            return False
        return len(result.get('Part', [])) == 0

    async def _abort_confirmed_by_list_parts(self, path, session_upload_id):
        """Ask LIST PARTS whether an abort that reported ``NoSuchUpload`` took effect.

        This is the same criterion the success path applies, asked of the same
        endpoint -- the point is precisely that it is *not* the DELETE's own
        word for it.

        A failure to obtain the answer returns ``False``.  The whole reason this
        check exists is that declaring success on an unestablished claim is what
        suppresses the "please remove the parts manually" warning; an
        unanswerable question has to keep the claim unestablished, not resolve
        it by default.  The caller then logs and retries, which is what it would
        have done without this branch at all.

        ``retry=0`` keeps this to exactly one round trip.  ``make_request``
        would otherwise retry 408/502/503/504 twice, sleeping 2s then 4s, so an
        unobtainable confirmation would stall the upload's error response for
        six seconds -- inside a check that falls through to the caller's own
        retry anyway.

        The bare ``except Exception`` below does swallow
        ``asyncio.CancelledError``: on Python 3.6 it derives from ``Exception``,
        not from ``BaseException``, so this clause and the four other broad
        handlers in this module all catch it.  That is accepted, not overlooked.
        It is harmless here only because ``waterbutler/`` contains no
        ``on_connection_close`` handler, no ``.cancel()`` call and no
        ``CancelledError`` reference at all, so nothing in the running service
        cancels this task.  On Python 3.8+ ``CancelledError`` moved under
        ``BaseException`` and the clause stops catching it, which is also the
        behaviour that would be wanted -- so this needs no code change, only
        re-checking if a cancellation path is ever introduced.
        """
        try:
            resp_xml, session_deleted = await self._list_uploaded_chunks(
                path, session_upload_id, retry=0)
            return session_deleted or self._no_parts_left(resp_xml)
        except Exception as err:
            logger.warning('Could not confirm a NoSuchUpload abort via ListParts. '
                           'upload_id=%s error_type=%s', session_upload_id, type(err).__name__)
            return False

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
                resp = await self._make_upload_request(
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

                # Abort is successful if the session has been deleted, or when
                # there is no part left.
                if session_deleted or self._no_parts_left(resp_xml):
                    is_aborted = True
                    break
            except exceptions.UploadError as err:
                # ``NoSuchUpload`` means the session is already gone, which is
                # exactly the state abort is trying to reach.  Retrying to the
                # cap and then reporting failure sends the user hunting for
                # parts that do not exist -- which is what happens whenever the
                # commit actually succeeded and only its response was
                # unreadable.
                #
                # But "the session is gone" is not the same claim as "the parts
                # are gone".  The docstring's own success criterion is LIST
                # PARTS returning 404 or an empty list, and ``NoSuchUpload`` on
                # the DELETE does not establish either -- a storage that has
                # dropped the session record while parts are still billable
                # would answer exactly this way.  So confirm it with the same
                # LIST PARTS check the success path uses, and fall through to
                # log-and-retry when the check disagrees.
                #
                # Only on the first iteration: the check is worth one round
                # trip, and re-asking it on every retry multiplies requests
                # against a storage that has already shown it is struggling.  A
                # first answer of "not confirmed" is not going to be overturned
                # by asking the same endpoint again a moment later.
                s3_error_code = self._parse_s3_error_body(err)[0]
                if s3_error_code == 'NoSuchUpload' and iteration_count == 0 and \
                        await self._abort_confirmed_by_list_parts(path, session_upload_id):
                    is_aborted = True
                    break
                self._log_abort_failure(err, session_upload_id, s3_error_code)
            except Exception as err:
                self._log_abort_failure(err, session_upload_id)

            iteration_count += 1

        if is_aborted:
            logger.debug('Multi-part upload has been successfully aborted: retries=%s '
                         'upload_id=%s', iteration_count, session_upload_id)
            return True

        logger.error('Multi-part upload has failed to abort: retries=%s '
                     'upload_id=%s', iteration_count, session_upload_id)
        return False

    async def _list_uploaded_chunks(self, path, session_upload_id, **request_kwargs):
        """This operation lists the parts that have been uploaded for a specific multipart upload.

        ``request_kwargs`` is passed through to ``make_request`` so a caller can
        override its retry budget; without one the core default applies.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
        """

        headers = {}
        query_parameters = {
            'Bucket': self.bucket_name,
            'Key': path.full_path,
            'UploadId': session_upload_id,
        }

        resp = await self._make_upload_request(
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
            **request_kwargs
        )
        session_deleted = resp.status == HTTPStatus.NOT_FOUND
        try:
            resp_xml = await resp.read()
        finally:
            await resp.release()

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

        # Every failure from here on is a failure of the commit itself, and the
        # commit is the one request whose outcome WaterButler cannot infer:
        # the parts are already stored, so "did the assemble happen?" is
        # answered only by the response.  Marking at both of the places the
        # commit can fail -- the request itself, and the reading of its answer
        # -- is what lets ``_translate_upload_error`` tell the user before they
        # upload the file a second time.  Which marks actually produce the
        # notice is decided there, not here.
        try:
            resp = await self._make_upload_request(
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
                # ``retry=0`` is a precondition of ``_commit_outcome_note``, not
                # a tuning choice.  ``make_request`` would otherwise re-send the
                # commit twice on 408/502/503/504 -- and a re-sent commit
                # consumes the UploadId, so a first attempt that *succeeded*
                # comes back from the second as ``NoSuchUpload``.  The code the
                # notice classifies would then describe the retry rather than
                # the upload, and the classification would be wrong in the
                # unrecoverable direction: silently telling the user nothing was
                # stored when it was.  Same reasoning as the abort confirmation
                # in ``_abort_confirmed_by_list_parts``.
                #
                # The cost is that a transient 503 now fails the upload instead
                # of being retried.  That failure carries the UNKNOWN notice, so
                # the user is told to check the file list -- which is better
                # than retrying silently and then asserting the wrong outcome.
                retry=0,
                # ``retry=0`` alone does not deliver that precondition: it stops
                # WaterButler's own retry loop, not the HTTP client's.  307 and
                # 308 mean "re-send, method and body intact", and aiohttp obeys
                # them by default -- a second commit POST that costs no retry
                # budget at all.  Everything said above about a re-sent commit
                # applies verbatim to that one.
                #
                # Refusing to follow means a storage that legitimately answers
                # the commit with a 307 now fails the upload.  That failure is
                # an unclassifiable status, so it carries the UNKNOWN notice:
                # the user is told to check the file list for a file that was in
                # fact stored.  Over-noticing is inside the tolerance this
                # provider accepts; asserting the wrong outcome is not.
                allow_redirects=False,
            )
        except Exception as err:
            _mark_commit_outcome_unknown(err)
            raise

        try:
            # ``read()`` belongs inside the try: a connection dropped mid-body
            # is exactly what a struggling storage does, and leaving the read
            # outside meant that case skipped the release entirely.
            response_body = await resp.read()
            # S3 reports CompleteMultipartUpload failures as HTTP 200 plus an
            # <Error> body, so this raises on what looks like a success -- and
            # that is exactly the path a quota-exhausted storage takes for
            # every upload.  Without the finally, each one leaks a connection.
            self._check_for_200_error(response_body, "CompleteMultipartUpload",
                                      exceptions.UploadError)
        except Exception as err:
            # ``Exception`` rather than ``UploadError``: the storage did answer
            # the commit and we could not read the answer, which arrives as an
            # aiohttp error.  Narrowing this to ``UploadError`` meant the one
            # case the notice exists for was the one case that never got it.
            _mark_commit_outcome_unknown(err)
            raise
        finally:
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
