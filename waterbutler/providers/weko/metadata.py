import os
import re
from waterbutler.core import metadata

from .schema import to_metadata


ITEM_PREFIX = 'weko:'


def _get_item_file_id(item):
    return 'item{}'.format(item.identifier)


def parse_item_file_id(part):
    m = re.match(r'^' + ITEM_PREFIX + r'item([0-9]+)$', part)
    if not m:
        return None
    return m.group(1)


def get_files(directory, relative=''):
    files = []
    for f in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, f)):
            files.append(os.path.join(relative, f) if len(relative) > 0 else f)
        elif os.path.isdir(os.path.join(directory, f)):
            for child in get_files(os.path.join(directory, f),
                                   os.path.join(relative, f)
                                   if len(relative) > 0 else f):
                files.append(child)
    return files


def _index_to_path_parts(target):
    if target.parent is None:
        return []
    parts = [target]
    while target.parent is not None and target.parent.parent is not None:
        target = target.parent
        parts.insert(0, target)
    return parts


def _index_to_path(target):
    r = '/'.join([ITEM_PREFIX + part.identifier for part in _index_to_path_parts(target)])
    if len(r) == 0:
        return r
    return r + '/'


def _index_to_materialized_path(target):
    r = '/'.join([part.title for part in _index_to_path_parts(target)])
    if len(r) == 0:
        return r
    return r + '/'


class BaseWEKOMetadata(metadata.BaseMetadata):
    @property
    def provider(self):
        return 'weko'

    @property
    def created_utc(self):
        return None


class WEKOFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    item = None

    def __init__(self, file, item, index):
        super().__init__(file)
        self.index = index
        self.item = item

    @property
    def file_id(self):
        return self.raw.filename

    @property
    def name(self):
        return self.raw.filename

    @property
    def content_type(self):
        return self.raw.format

    @property
    def identifier(self):
        return self.raw.filename

    @property
    def path(self):
        return '/' + _index_to_path(self.index) + ITEM_PREFIX + _get_item_file_id(self.item) + '/' + self.identifier

    @property
    def materialized_path(self):
        return '/' + _index_to_materialized_path(self.index) + self.item.primary_title + '/' + self.identifier

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw.version_id

    @property
    def extra(self):
        return {
            'weko': 'file',
            'itemId': _get_item_file_id(self.item),
            'metadata': None,
        }


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    index = None

    def __init__(self, client, raw, index, provider_name, metadata_schema_id):
        super().__init__(raw)
        self.client = client
        self.index = index
        self.provider_name = provider_name
        self.metadata_schema_id = metadata_schema_id

    @property
    def file_id(self):
        return _get_item_file_id(self.raw)

    @property
    def name(self):
        return self.raw.primary_title

    @property
    def content_type(self):
        return None

    @property
    def identifier(self):
        return ITEM_PREFIX + self.file_id

    @property
    def materialized_path(self):
        return '/' + _index_to_materialized_path(self.index) + self.name + '/'

    @property
    def path(self):
        return '/' + _index_to_path(self.index) + self.identifier + '/'

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.file_id

    @property
    def extra(self):
        return {
            'weko': 'item',
            'weko_web_url': self.client.get_item_records_url(str(self.raw.identifier)),
            'fileId': self.file_id,
            'metadata': self._to_metadata(),
        }

    def _to_metadata(self):
        if self.metadata_schema_id is None:
            return None
        return {
            'folder': False,
            'generated': False,
            'path': self.provider_name + self.path,
            'items': [
                {
                    'active': True,
                    'data': to_metadata(self.metadata_schema_id, self.raw),
                    'schema': self.metadata_schema_id,
                    'readonly': True,
                }
            ],
        }


class WEKOIndexMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    def __init__(self, client, raw):
        super().__init__(raw)
        self.client = client

    @property
    def name(self):
        return self.raw.title

    @property
    def identifier(self):
        return ITEM_PREFIX + self.raw.identifier

    @property
    def materialized_path(self):
        return '/' + _index_to_materialized_path(self.raw)

    @property
    def path(self):
        return '/' + _index_to_path(self.raw)

    @property
    def extra(self):
        return {
            'weko': 'index',
            'weko_web_url': self.client.get_index_items_url(self.raw.identifier),
            'indexId': self.raw.identifier,
            'metadata': None,
        }


class WEKODraftFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    file = None

    def __init__(self, file, index):
        super().__init__(file)
        self.index = index

    @property
    def name(self):
        return self.raw.name

    @property
    def content_type(self):
        return self.raw.content_type

    @property
    def identifier(self):
        return self.raw.name

    @property
    def path(self):
        return '/' + _index_to_path(self.index) + self.raw.name

    @property
    def materialized_path(self):
        return '/' + _index_to_materialized_path(self.index) + self.raw.name

    @property
    def size(self):
        return self.raw.size

    @property
    def modified(self):
        return self.raw.modified

    @property
    def etag(self):
        return self.raw.etag

    @property
    def extra(self):
        r = {
            'weko': 'draft',
            'index': self.index.identifier,
            'source': {
                'provider': self.raw.provider,
                'path': self.raw.path,
                'materialized_path': self.raw.materialized_path,
            },
        }
        r.update(self.raw.extra)
        return r
