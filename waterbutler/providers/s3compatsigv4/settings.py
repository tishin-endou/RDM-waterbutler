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
# vendor uses a different error code.  ``get_object`` is required here: plain
# ``get`` returns the raw string when the value comes from an envvar, which
# would silently turn the membership test into substring matching.


QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS = [
    'QuotaExceeded',
    'XMinioAdminBucketQuotaExceeded',
    'XMinioStorageFull',
]


def _read_error_codes():
    """Read the configured error-code list, surviving anything the envvar holds.

    ``SettingsDict.get_object`` calls ``json.loads`` with no ``try``, so an
    envvar that is not valid JSON raises while this module is being imported.
    Normalising ``get_object``'s *return value* -- which is what
    ``_normalise_error_codes`` does -- cannot help, because the argument is
    evaluated first.

    An import failure here is not the loud failure it looks like.  stevedore
    turns the entry-point load error into a ``RuntimeError``, which
    ``waterbutler/core/utils.py`` converts into ``ProviderNotFound``: the
    process stays up, every other provider keeps working, and s3compatsigv4
    answers HTTP 404 to everything.  A typo in a quota code would surface as a
    symptom with no visible connection to its cause.  Falling back to the
    defaults keeps quota detection working and puts the cause in the log.
    """
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
    """Coerce a configured error-code list into a ``frozenset`` of ``str``.

    ``SettingsDict.get_object`` is ``json.loads`` with no type check
    (``waterbutler/settings.py``), so the value can decode to any JSON type.
    The only consumer is a ``code in codes`` membership test, which degrades
    silently for every type but a collection of strings:

    * a bare ``str`` makes the test substring matching (``'Quota'`` would match
      a configured ``'QuotaExceeded'``) -- and handing it to ``frozenset``
      instead explodes it into one entry per character, so nothing matches;
    * a number is not iterable at all, so ``frozenset`` raises ``TypeError``.

    Normalising here rather than at the point of use is deliberate: this module
    is the only place the raw configuration exists, so fixing the type here
    means no caller can observe the un-normalised value.

    A scalar is coerced rather than rejected.  Raising would happen at import
    time and take the whole provider down over a quota-code typo, which is a
    far worse outcome than running with the single code the operator meant;
    the warning is what makes the misconfiguration visible.

    A mapping is rejected rather than coerced.  ``dict`` satisfies ``Iterable``,
    so it would slip past the scalar branch and be reduced to its *keys* --
    a result that is indistinguishable from a working configuration until a
    quota error goes unrecognised in production.  There is no reading of
    ``{"QuotaExceeded": 507}`` that makes the operator's intent unambiguous.
    """
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
