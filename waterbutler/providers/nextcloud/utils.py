import xml.etree.ElementTree as ET
from urllib import parse
from waterbutler.core import exceptions
from waterbutler.providers.nextcloud.metadata import NextcloudFileMetadata
from waterbutler.providers.nextcloud.metadata import NextcloudFolderMetadata


def strip_dav_path(path):
    """Removes the leading "remote.php/webdav" path from the given path.

    :param str path: path containing the remote DAV path "remote.php/webdav"
    :return: path stripped of the remote DAV path
    :rtype: str
    """
    if 'remote.php/webdav' in path:
        return path.split('remote.php/webdav')[1]
    return path


async def parse_dav_response(provider, content, folder, skip_first=False):
    """Parses the xml content returned from WebDAV and returns the metadata equivalent. By default,
    WebDAV returns the metadata of the queried item first. If the root directory is selected, then
    WebDAV returns server information first. Hence, a ``skip_first`` option is included in the
    parameters.

    :param str provider: Provider name
    :param str content: Body content from WebDAV response
    :param str folder: Parent folder for content
    :param bool skip_first: strip off the first result of the WebDAV response
    :return: List of metadata responses.
    """
    items = []
    tree = ET.fromstring(content)

    if skip_first:
        tree = tree[1:]

    for child in tree:
        href = ''
        try:
            href = parse.unquote(strip_dav_path(child.find('{DAV:}href').text))
        except AttributeError:
            raise exceptions.NotFoundError(folder)
        file_type = 'file'
        if href[-1] == '/':
            file_type = 'dir'

        file_attrs = {}
        attrs = child.find('{DAV:}propstat').find('{DAV:}prop')

        for attr in attrs:
            file_attrs[attr.tag] = attr.text

        if file_type == 'file':
            items.append(NextcloudFileMetadata(href, folder, provider, file_attrs))
        else:
            items.append(NextcloudFolderMetadata(href, folder, provider, file_attrs))
    return items


async def parse_checksum_response(content):
    hashes = {}

    root = ET.fromstring(content)
    meta = root[0]
    statuscode = meta[1]
    CHECKSUM_API_SUCCESS = '200'
    if statuscode.text == CHECKSUM_API_SUCCESS:
        data = root[1]
        for h in data[0]:
            hashes[h.tag] = h.text

    return hashes
