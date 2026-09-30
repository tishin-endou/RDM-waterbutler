import logging
from collections.abc import Iterable, Mapping

from waterbutler import settings

logger = logging.getLogger(__name__)

config = settings.child('S3COMPAT_PROVIDER_CONFIG')


TEMP_URL_SECS = int(config.get('TEMP_URL_SECS', 100))

CONTIGUOUS_UPLOAD_SIZE_LIMIT = int(config.get('CONTIGUOUS_UPLOAD_SIZE_LIMIT', 128000000))  # 128 MB

CHUNK_SIZE = int(config.get('CHUNK_SIZE', 64000000))  # 64 MB

CHUNKED_UPLOAD_MAX_ABORT_RETRIES = int(config.get('CHUNKED_UPLOAD_MAX_ABORT_RETRIES', 2))

# S3-compatible storages return different XML error codes when the storage-side
# quota / capacity has been exhausted.  Well-known ones are listed as defaults:
# - 'QuotaExceeded': generic S3-compatible storages
# - 'XMinioAdminBucketQuotaExceeded': MinIO with a bucket quota configured
# - 'XMinioStorageFull': MinIO when the underlying disk is full (S3 data path)
# This list is not exhaustive, so ``_translate_upload_error`` additionally
# treats HTTP 507 as a quota failure regardless of the error code.
# Deployments can replace this list via the provider config when their storage
# vendor uses a different error code.
QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS = [
    'QuotaExceeded',
    'XMinioAdminBucketQuotaExceeded',
    'XMinioStorageFull',
]


def _read_error_codes():
    """Read the configured error-code list, falling back to defaults on bad JSON."""
    try:
        return config.get_object('QUOTA_EXCEEDED_ERROR_CODES',
                                 QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS)
    except (ValueError, TypeError) as err:
        # ``JSONDecodeError`` is a ``ValueError``.  ``TypeError`` covers a
        # non-string, non-JSON value arriving from a config file.
        logger.warning('S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES is not valid JSON '
                       '(%s: %s); falling back to the built-in defaults.  A bare error code '
                       'must be quoted, e.g. \'["QuotaExceeded"]\'.',
                       type(err).__name__, err)
        return QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS


def _normalise_error_codes(configured):
    """Coerce a configured value into a ``frozenset`` of ``str``.

    Scalar and None values are coerced with a warning; mappings fall back to
    defaults.
    """
    if configured is None:
        return frozenset(QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS)
    if isinstance(configured, Mapping):
        logger.warning('S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES is a mapping; '
                       'expected a list of error codes.  Falling back to the built-in '
                       'defaults.')
        configured = QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS
    elif isinstance(configured, str) or not isinstance(configured, Iterable):
        logger.warning('S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES is a scalar '
                       '(%s); expected a list.  Treating it as a single error code.',
                       type(configured).__name__)
        configured = (configured,)
    return frozenset(str(code) for code in configured)


QUOTA_EXCEEDED_ERROR_CODES = _normalise_error_codes(_read_error_codes())
