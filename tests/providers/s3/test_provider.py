import os
import io
import xml
import json
import time
import base64
import hashlib
import aiohttpretty
from http import client
from urllib import parse
from unittest import mock

import pytest
# from boto.compat import BytesIO
# from boto.utils import compute_md5

from waterbutler.providers.s3 import S3Provider
from waterbutler.core.path import WaterButlerPath
from waterbutler.core import streams, metadata, exceptions
from waterbutler.providers.s3 import settings as pd_settings

from tests.utils import MockCoroutine
from tests.providers.s3.fixtures import (auth,
                                         settings,
                                         credentials,
                                         file_content,
                                         folder_metadata,
                                         folder_metadata,
                                         version_metadata,
                                         create_session_resp,
                                         folder_and_contents,
                                         complete_upload_resp,
                                         file_header_metadata,
                                         file_metadata_object,
                                         folder_item_metadata,
                                         generic_http_403_resp,
                                         generic_http_404_resp,
                                         list_parts_resp_empty,
                                         folder_empty_metadata,
                                         single_version_metadata,
                                         revision_metadata_object,
                                         upload_parts_headers_list,
                                         list_parts_resp_not_empty,
                                         folder_key_metadata_object,
                                         folder_single_item_metadata,
                                         file_metadata_headers_object,
                                         )


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    prov = S3Provider(auth, credentials, settings)
    prov._check_region = MockCoroutine()

    async def _gen_presigned(path, method='head_object', query_parameters=None, default_params=True):
        clean = path.lstrip('/')
        if clean:
            return f'https://that-kerning.s3.amazonaws.com/{clean}'
        return 'https://that-kerning.s3.amazonaws.com/'

    prov.generate_generic_presigned_url = _gen_presigned

    async def _check_key(path, expects=(200,), query_parameters=None):
        url = f'https://that-kerning.s3.amazonaws.com/{path}'
        return await prov.make_request('HEAD', url, expects=expects, throws=exceptions.MetadataError)

    prov.check_key_existence = _check_key

    return prov


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


def location_response(location):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        '{}</LocationConstraint>'
    ).format(location)


def list_objects_response(keys, truncated=False):
    response = '''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>'''

    response += '<IsTruncated>' + str(truncated).lower() + '</IsTruncated>'
    response += ''.join(map(
        lambda x: f'<Contents><Key>{x}</Key></Contents>',
        keys
    ))

    response += '</ListBucketResult>'

    return response.encode('utf-8')


def bulk_delete_body(keys):
    payload = '<?xml version="1.0" encoding="UTF-8"?>'
    payload += '<Delete>'
    payload += ''.join(map(
        lambda x: f'<Object><Key>{x}</Key></Object>',
        keys
    ))
    payload += '</Delete>'
    payload = payload.encode('utf-8')

    md5 = base64.b64encode(hashlib.md5(payload).digest())
    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5.decode('ascii'),
        'Content-Type': 'text/xml',
    }

    return (payload, headers)


def list_upload_chunks_body(parts_metadata):
    payload = b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Bucket>example-bucket</Bucket>
            <Key>example-object</Key>
            <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
            <Initiator>
                <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>
                <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-b288870f11xx</DisplayName>
            </Initiator>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>someName</DisplayName>
            </Owner>
            <StorageClass>STANDARD</StorageClass>
            <PartNumberMarker>1</PartNumberMarker>
            <NextPartNumberMarker>3</NextPartNumberMarker>
            <MaxParts>2</MaxParts>
            <IsTruncated>false</IsTruncated>
            <Part>
                <PartNumber>2</PartNumber>
                <LastModified>2010-11-10T20:48:34.000Z</LastModified>
                <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
                <Size>10485760</Size>
            </Part>
            <Part>
                <PartNumber>3</PartNumber>
                <LastModified>2010-11-10T20:48:33.000Z</LastModified>
                <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
                <Size>10485760</Size>
            </Part>
        </ListPartsResult>
    '''

    # md5 = compute_md5(BytesIO(payload))
    # md5 = compute_md5(payload)
    md5 = hashlib.md5(payload)

    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5.hexdigest(),
        'Content-Type': 'text/xml',
    }

    return payload, headers


def build_folder_params(path):
    return {'prefix': path.path, 'delimiter': '/'}

class TestRegionDetection:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("region_name,expected_region", [
        # ('',               's3.amazonaws.com'),
        ('EU',             'eu-west-1'),
        ('us-east-2',      'us-east-2'),
        ('us-west-1',      'us-west-1'),
        ('us-west-2',      'us-west-2'),
        ('ca-central-1',   'ca-central-1'),
        ('eu-central-1',   'eu-central-1'),
        ('eu-west-2',      'eu-west-2'),
        ('ap-northeast-1', 'ap-northeast-1'),
        ('ap-northeast-2', 'ap-northeast-2'),
        ('ap-south-1',     'ap-south-1'),
        ('ap-southeast-1', 'ap-southeast-1'),
        ('ap-southeast-2', 'ap-southeast-2'),
        ('sa-east-1',      'sa-east-1'),
    ])
    async def test_region_host(self, auth, credentials, settings, region_name, expected_region, mock_time):
        provider = S3Provider(auth, credentials, settings)
        region_url = await provider.generate_generic_presigned_url(
            '', method='get_bucket_location', query_parameters={'Bucket': settings['bucket']},  default_params=False
        )
        aiohttpretty.register_uri('GET', region_url, status=200, body=location_response(region_name),
                                  match_querystring=False)
        await provider._check_region()
        assert provider.region == expected_region
        # provider = S3Provider(auth, credentials, settings)
        # await provider._check_region()
        # await provider._check_region()
        # res = await provider._get_bucket_region()
        # # region_url = provider.bucket.generate_url(
        # #     100,
        # #     'GET',
        # #     query_parameters={'location': ''},
        # # )
        # region_url = 'https://s3.amazonaws.com/that-kerning?location=&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=Dont%20dead%2F20250526%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250526T134653Z&X-Amz-Expires=100&X-Amz-SignedHeaders=host&X-Amz-Signature=80f8426c4fc6d0af68bd3e52a553c9e4d838144b9a70600aff507f70056696f1 '
        # aiohttpretty.register_uri('GET',
        #                           region_url,
        #                           status=200,
        #                           body=location_response(region_name))
        #
        # await provider._check_region()
        # assert provider.connection.host == host


class TestInitialization:

    @pytest.mark.parametrize(('provider_settings', 'expected_base_folder'), [
        ({'id': 'that-kerning:/my-subfolder/'}, 'my-subfolder/'),
        ({'id': 'that-kerning'}, ''),
        ({'id': None}, ''),
    ])
    def test_base_folder_parsing(self, auth, credentials, settings, provider_settings, expected_base_folder):
        provider_settings = dict(settings, **provider_settings)
        if provider_settings['id'] is None:
            del provider_settings['id']

        provider = S3Provider(auth, credentials, provider_settings)

        assert provider.base_folder == expected_base_folder


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_header_metadata, mock_time):
        file_path = 'foobah'

        root_listing_url = 'https://that-kerning.s3.amazonaws.com/my-subfolder/'
        file_head_url = f'https://that-kerning.s3.amazonaws.com/my-subfolder/{file_path}'
        bucket_listing_url = 'https://that-kerning.s3.amazonaws.com/'

        aiohttpretty.register_uri(
            'GET',
            bucket_listing_url,
            body=b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><Prefix>my-subfolder/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>',
            headers={'Content-Type': 'application/xml'},
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            file_head_url,
            headers=file_header_metadata,
            match_querystring=False,
        )

        assert WaterButlerPath('/my-subfolder/', prepend=None) == await provider.validate_v1_path('/')

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file_with_subfolder(self, provider, file_header_metadata, mock_time):
        file_path = '/foobah'

        listing_url = 'https://that-kerning.s3.amazonaws.com/'
        file_head_url = f'https://that-kerning.s3.amazonaws.com/my-subfolder{file_path}'

        aiohttpretty.register_uri(
            'GET',
            listing_url,
            headers=file_header_metadata,
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            file_head_url,
            headers=file_header_metadata,
            match_querystring=False,
        )

        assert WaterButlerPath('/my-subfolder/') == await provider.validate_v1_path('/')
        wb_path_v1 = await provider.validate_v1_path(file_path)
        wb_path_v0 = await provider.validate_path(file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time):
        folder_path = '/Photos'

        listing_url = 'https://that-kerning.s3.amazonaws.com/'

        aiohttpretty.register_uri(
            'GET',
            listing_url,
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'},
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            f'https://that-kerning.s3.amazonaws.com/my-subfolder{folder_path}',
            status=404,
            match_querystring=False,
        )

        wb_path_v1 = await provider.validate_v1_path(folder_path + '/')
        wb_path_v0 = await provider.validate_path(folder_path + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_normal_name(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/path.txt')
        assert path.name == 'path.txt'
        assert path.parent.name == 'a'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_folder(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert path.name == 'folder'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_subfolder(self, provider, mock_time):
        path = await provider.validate_path('/')
        assert path.name == 'my-subfolder'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'de', auto_length=True, status=206,
                                  match_querystring=False)

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'de'
        assert aiohttpretty.has_call(method='GET', uri=url, headers={'Range': 'bytes=0-1'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path, revision='someversion')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'muhtriangle'),
        (None,       'muhtriangle'),
    ])
    async def test_download_with_display_name(self, provider, mock_time, display_name_arg,
                                              expected_name):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path, display_name=display_name_arg)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, status=404, match_querystring=False)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/'))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_to_subfolder_as_root(self,
                                               provider,
                                               file_content,
                                               file_stream,
                                               file_header_metadata,
                                               mock_time
                                               ):

        provider.settings['id'] = 'the-bucket:/my-subfolder/'
        path = WaterButlerPath('/my-subfolder/foobah')

        content_md5 = hashlib.md5(file_content).hexdigest()

        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata,
                                  match_querystring=False)
        header = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.path == '/foobah'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self,
                                 provider,
                                 file_content,
                                 file_stream,
                                 file_header_metadata,
                                 mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata,
                                  match_querystring=False)
        header = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_encrypted(self,
                                    provider,
                                    file_content,
                                    file_stream,
                                    file_header_metadata,
                                    mock_time):

        # Set trigger for encrypt_key=True in s3.provider.upload
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        headers={'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.extra['encryption'] == 'AES256'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_chunked(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        provider._chunked_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._chunked_upload.assert_called_with(file_stream, path)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_contiguous(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 10
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        provider._contiguous_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._contiguous_upload.assert_called_with(file_stream, path)

        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_no_encryption(self, provider,
                                                                      create_session_resp,
                                                                      mock_time):
        path = WaterButlerPath('/foobah')
        init_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200,
                                  match_querystring=False)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_with_encryption(self, provider,
                                                                        create_session_resp,
                                                                        mock_time):
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah')
        init_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200,
                                  match_querystring=False)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts(self, provider, file_stream,
                                               upload_parts_headers_list):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts_remainder(self, provider,
                                                         upload_parts_headers_list):

        file_stream = streams.StringStream('abcdefghijklmnopqrst')
        assert file_stream.size == 20
        provider.CHUNK_SIZE = 9

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        provider._upload_part.assert_has_calls([
            mock.call(file_stream, path, upload_id, 1, 9),
            mock.call(file_stream, path, upload_id, 2, 9),
            mock.call(file_stream, path, upload_id, 3, 2),
        ])
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_part(self, provider, file_stream,
                                              upload_parts_headers_list,
                                              mock_time):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        upload_part_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        # aiohttp resp headers use upper case
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}
        aiohttpretty.register_uri('PUT', upload_part_url, status=200, headers=part_headers,
                                  match_querystring=False)

        part_metadata = await provider._upload_part(file_stream, path, upload_id, chunk_number,
                                                    provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url)
        assert part_headers == part_metadata

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload(self, provider,
                                                            upload_parts_headers_list,
                                                            complete_upload_resp, mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        # aiohttp resp headers are upper case
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += f'<PartNumber>{i+1}</PartNumber>'  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        complete_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri(
            'POST',
            complete_url,
            status=200,
            body=complete_upload_resp,
            match_querystring=False,
        )

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_session_deleted(self, provider, generic_http_404_resp,
                                                        mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_empty(self, provider, list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_session_not_found(self,
                                                          provider,
                                                          generic_http_404_resp,
                                                          mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_empty_list(self,
                                                   provider,
                                                   list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-file')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', url, status=200, match_querystring=False)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_comfirm_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/')

        provider.delete_s3_bucket_folder_objects = MockCoroutine()

        with pytest.raises(exceptions.DeleteError):
            await provider.delete(path)

        await provider.delete(path, confirm_delete=1)

        provider.delete_s3_bucket_folder_objects.assert_called_once_with(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/some-folder/')

        provider.delete_s3_bucket_folder_objects = MockCoroutine()

        await provider.delete(path)

        provider.delete_s3_bucket_folder_objects.assert_called_once_with(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_item_folder_delete(self,
                                             provider,
                                             folder_single_item_metadata,
                                             mock_time):
        path = WaterButlerPath('/single-thing-folder/')

        provider.delete_s3_bucket_folder_objects = MockCoroutine()

        await provider.delete(path)

        provider.delete_s3_bucket_folder_objects.assert_called_once_with(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_folder_delete(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/empty-folder/')
        provider.delete_s3_bucket_folder_objects = MockCoroutine()
        await provider.delete(path)  # Should succeed without error
        provider.delete_s3_bucket_folder_objects.assert_called_once_with(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_large_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-folder/')

        provider.delete_s3_bucket_folder_objects = MockCoroutine()

        await provider.delete(path)

        provider.delete_s3_bucket_folder_objects.assert_called_once_with(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_accepts_url(self, provider, mock_time):
        path = WaterButlerPath('/my-image')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'content', auto_length=True,
                                  match_querystring=False)
        result = await provider.download(path)
        content = await result.read()
        assert content == b'content'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/darp/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'
        assert result[2].extra['hashes']['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_self_listing(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/thisfolder/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_and_contents if isinstance(folder_and_contents, bytes) else folder_and_contents.encode('utf-8'),
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata_folder_item(self, provider, folder_item_metadata, mock_time):
        path = WaterButlerPath('/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_item_metadata if isinstance(folder_item_metadata, bytes) else folder_item_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/this-is-not-the-root/')
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_empty_metadata if isinstance(folder_empty_metadata, bytes) else folder_empty_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        aiohttpretty.register_uri('HEAD', metadata_url, headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata,
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_lastest_revision(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata,
                                  match_querystring=False)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, status=404, match_querystring=False)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self,
                          provider,
                          file_content,
                          file_stream,
                          file_header_metadata,
                          mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        headers = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers,
                                  match_querystring=False),

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self,
                                            provider,
                                            file_stream,
                                            file_header_metadata,
                                            mock_time):
        path = WaterButlerPath('/foobah')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"bad hash"'},
                                  match_querystring=False)

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raise_409(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "alreadyexists", because a file or '
                                   'folder already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        create_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('GET', url, status=404, match_querystring=False)
        aiohttpretty.register_uri('PUT', create_url, status=403, match_querystring=False)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET', url, status=403, match_querystring=False)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        create_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('GET', url, status=404, match_querystring=False)
        aiohttpretty.register_uri('PUT', create_url, status=200, match_querystring=False)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.skip('Mocking too complicated')
    async def test_intra_copy(self, provider, file_header_metadata, mock_time):
        source_path = WaterButlerPath('/source')
        dest_path = WaterButlerPath('/dest')
        metadata_url = provider.bucket.new_key('/my-subfolder/' + dest_path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)

        header_path = '/' + os.path.join(provider.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(header_path)}

        url = provider.bucket.new_key('/my-subfolder/' + dest_path.path).generate_url(100, 'PUT', headers=headers)
        aiohttpretty.register_uri('PUT', url, status=200)

        metadata, exists = await provider.intra_copy(provider, source_path, dest_path)


        provider._check_region.assert_called()

        assert metadata.kind == 'file'
        assert not exists
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=url, headers=headers)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time):
        path = WaterButlerPath('/my-image.jpg')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=version_metadata if isinstance(version_metadata, bytes) else version_metadata.encode('utf-8'),
                                  status=200, match_querystring=False)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 3

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_version_metadata(self, provider, single_version_metadata, mock_time):
        path = WaterButlerPath('/single-version.file')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET',
                                  url,
                                  body=single_version_metadata if isinstance(single_version_metadata, bytes) else single_version_metadata.encode('utf-8'),
                                  status=200,
                                  match_querystring=False)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 1

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url)

    def test_can_intra_move(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert provider.can_intra_move(provider)
        assert provider.can_intra_move(provider, file_path)
        assert not provider.can_intra_move(provider, folder_path)

    def test_can_intra_copy(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert provider.can_intra_copy(provider)
        assert provider.can_intra_copy(provider, file_path)
        assert not provider.can_intra_copy(provider, folder_path)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
