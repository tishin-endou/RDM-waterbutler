from waterbutler import settings

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
QUOTA_EXCEEDED_ERROR_CODES = frozenset(config.get_object('QUOTA_EXCEEDED_ERROR_CODES', [
    'QuotaExceeded',
    'XMinioAdminBucketQuotaExceeded',
    'XMinioStorageFull',
]))
