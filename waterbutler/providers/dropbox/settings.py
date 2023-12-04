from waterbutler import settings

config = settings.child('DROPBOX_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.dropboxapi.com/2')

BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'https://content.dropboxapi.com/2/')

CONTIGUOUS_UPLOAD_SIZE_LIMIT = int(config.get('CONTIGUOUS_UPLOAD_SIZE_LIMIT', 150000000))  # 150 MB

CHUNK_SIZE = int(config.get('CHUNK_SIZE', 4000000))  # 4 MB

# An optional integer specifying the number of failed requests retries for a simple Dropbox API call.
# Specify usage for `DropboxProvider.dropbox_request()` and `BaseProvider.make_request()` usages
# in the DropboxProvider and DropboxBusinessProvider
RETRY = 5
