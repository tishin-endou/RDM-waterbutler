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
    index_identifier = None
    index_path = None
    index_materialized_path = None
    item_file_id = None
    item_title = None

    def __init__(self, file, item, index):
        super().__init__({
            'filename': file.filename,
            'format': file.format,
            'version_id': file.version_id
        })
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(index)
        self.index_materialized_path = _index_to_materialized_path(index)
        self.item_file_id = _get_item_file_id(item)
        self.item_title = item.primary_title

    @property
    def file_id(self):
        return self.raw['filename']

    @property
    def name(self):
        return self.raw['filename']

    @property
    def content_type(self):
        return self.raw['format']

    @property
    def identifier(self):
        return self.raw['filename']

    @property
    def path(self):
        return '/' + self.index_path + ITEM_PREFIX + self.item_file_id + '/' + self.identifier

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self.item_title + '/' + self.identifier

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw['version_id']

    @property
    def extra(self):
        return {
            'weko': 'file',
            'itemId': self.item_file_id,
            'metadata': None,
        }


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    file_id = None
    provider_name = None
    metadata_schema_id = None
    index_identifier = None
    index_path = None
    index_materialized_path = None
    item_file_id = None
    weko_web_url = None

    def __init__(self, client, raw, index, provider_name, metadata_schema_id):
        super().__init__({
            'primary_title': raw.primary_title,
            'metadata': raw.raw['metadata'],
        })
        self.file_id = _get_item_file_id(raw)
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(index)
        self.index_materialized_path = _index_to_materialized_path(index)
        self.provider_name = provider_name
        self.metadata_schema_id = metadata_schema_id
        self.weko_web_url = client.get_item_records_url(str(raw.identifier))

    @property
    def name(self):
        return self.raw['primary_title']

    @property
    def content_type(self):
        return None

    @property
    def identifier(self):
        return ITEM_PREFIX + self.file_id

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self.name + '/'

    @property
    def path(self):
        return '/' + self.index_path + self.identifier + '/'

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
            'weko_web_url': self.weko_web_url,
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
    index_identifier = None
    index_path = None
    index_materialized_path = None
    weko_web_url = None

    def __init__(self, client, raw):
        super().__init__({
            'title': raw.title,
        })
        self.index_identifier = raw.identifier
        self.index_path = _index_to_path(raw)
        self.index_materialized_path = _index_to_materialized_path(raw)
        self.weko_web_url = client.get_index_items_url(raw.identifier)

    @property
    def name(self):
        return self.raw['title']

    @property
    def identifier(self):
        return ITEM_PREFIX + self.index_identifier

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path

    @property
    def path(self):
        return '/' + self.index_path

    @property
    def extra(self):
        return {
            'weko': 'index',
            'weko_web_url': self.weko_web_url,
            'indexId': self.index_identifier,
            'metadata': None,
        }


class BaseWEKODraftMetadata(BaseWEKOMetadata):
    index_identifier = None
    index_path = None
    index_materialized_path = None
    index_folder = None

    def __init__(self, file, index_folder, index):
        super().__init__(file)
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(index)
        self.index_materialized_path = _index_to_materialized_path(index)
        self.index_folder = index_folder

    @property
    def extra(self):
        r = {
            'weko': 'draft',
            'index': self.index_identifier,
            'source': {
                'provider': self.raw.provider,
                'path': self.raw.path,
                'materialized_path': self.raw.materialized_path,
            },
        }
        r.update(self.raw.extra)
        return r

    @property
    def path(self):
        return '/' + self.index_path + self._relative_path

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self._relative_path

    @property
    def _relative_path(self):
        base_path = self.index_folder.materialized_path
        item_path = self.raw.materialized_path
        if not item_path.startswith(base_path):
            raise ValueError(f'Unexpected path: base_path={base_path}, item_path={item_path}')
        return item_path[len(base_path):]


class WEKODraftFolderMetadata(BaseWEKODraftMetadata, metadata.BaseFolderMetadata):
    @property
    def name(self):
        return self.raw.name


class WEKODraftFileMetadata(BaseWEKODraftMetadata, metadata.BaseFileMetadata):
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
    def size(self):
        return self.raw.size

    @property
    def modified(self):
        return self.raw.modified

    @property
    def etag(self):
        return self.raw.etag
