import io
import xml
import json
import time
import base64
import hashlib
import asyncio
import logging
import datetime

import aiohttp
import aiohttpretty
import xmltodict
from aiohttp import web
from http import client
from http import HTTPStatus
from urllib import parse
from unittest import mock

import pytest
from boto.compat import BytesIO
from boto.utils import compute_md5

from waterbutler.core import streams, metadata, exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.s3compatsigv4 import S3CompatSigV4Provider
from waterbutler.providers.s3compatsigv4 import settings as pd_settings
from waterbutler.providers.s3compatsigv4 import provider as pd_provider

PROVIDER_LOGGER = pd_provider.__name__


def storage_error(message, code=403, exception_type=exceptions.UploadError):
    """Build a tagged storage-origin UploadError for test injection."""
    return pd_provider._mark_storage_response(exception_type(message, code=code))


from tests.utils import MockCoroutine
from collections import OrderedDict
from waterbutler.providers.s3compatsigv4.metadata import (S3CompatSigV4Revision,
                                                     S3CompatSigV4FileMetadata,
                                                     S3CompatSigV4FolderMetadata,
                                                     S3CompatSigV4FolderKeyMetadata,
                                                     S3CompatSigV4FileMetadataHeaders,
                                                     )
from hmac import compare_digest

# Independent copy so a deleted row cannot delete its own test.
DEFINITIVE_REJECTION_CODES = [
    'AccessDenied',
    'InvalidPart',
    'InvalidPartOrder',
    'EntityTooSmall',
    'EntityTooLarge',
    'MalformedXML',
    'SignatureDoesNotMatch',
    'InvalidAccessKeyId',
    'NoSuchBucket',
]


def commit_error_xml(error_code):
    """An S3 error body; ``None`` yields a body with no ``<Code>`` element."""
    if error_code is None:
        return ('<?xml version="1.0" encoding="UTF-8"?>'
                '<Error><Message>boom</Message></Error>')
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<Error><Code>{}</Code><Message>boom</Message></Error>'.format(error_code))


def arrange_commit_failure(provider, transport, error_code):
    """Fail only ``_complete_multipart_upload``, in the shape of ``transport``."""
    if transport == 'direct_4xx':
        provider.make_request = MockCoroutine(side_effect=exceptions.UploadError(
            {'response': commit_error_xml(error_code)}, code=400))
    elif transport == 'direct_5xx':
        provider.make_request = MockCoroutine(side_effect=exceptions.UploadError(
            {'response': commit_error_xml(error_code)}, code=500))
    elif transport == 'complete_200_error':
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=commit_error_xml(error_code).encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)
    elif transport == 'disconnect':
        provider.make_request = MockCoroutine(
            side_effect=aiohttp.ServerDisconnectedError(commit_error_xml(error_code)))
    elif transport == 'broken_xml':
        # Truncated body: code string present but unparsable -- substring match must not pick it up.
        truncated = commit_error_xml(error_code)[:-12].encode('utf-8')
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=truncated)
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)
    else:  # pragma: no cover - a mistyped parameter must not pass silently
        raise AssertionError('unknown transport: {}'.format(transport))


def arrange_chunked_commit(provider):
    """Set up ``_chunked_upload`` so that only the commit fails."""
    provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
    provider.CHUNK_SIZE = 2
    provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
    provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
    provider._abort_chunked_upload = MockCoroutine(return_value=True)


class commit_server:
    """Real-socket test server for commit endpoint tests."""

    def __init__(self, provider, app):
        self.provider = provider
        self.app = app
        self.runner = web.AppRunner(app)
        self.url = None

    async def __aenter__(self):
        await self.runner.setup()
        try:
            site = web.TCPSite(self.runner, '127.0.0.1', 0)
            await site.start()
            sockets = site._server.sockets
            assert sockets, 'the test server bound no socket'
            self.url = 'http://127.0.0.1:{}/first'.format(sockets[0].getsockname()[1])
        except Exception:
            # ``__aexit__`` is not called when ``__aenter__`` raises.
            await self.runner.cleanup()
            raise
        return self

    async def __aexit__(self, *exc_info):
        first = None
        try:
            # One failing close must not strand the rest.
            for session in self.provider.session_list:
                try:
                    await session.close()
                except Exception as err:
                    # Raise the first failure, but finish closing them all.
                    first = first if first is not None else err
        finally:
            # The listening socket comes down even if a session close fails.
            await self.runner.cleanup()
        if first is not None:
            raise first
        return False


def set_chunked_limits(provider, file_stream):
    """Set provider limits so that file_stream (6 bytes) routes to _chunked_upload."""
    assert file_stream.size == 6
    provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
    provider.CHUNK_SIZE = 2


@pytest.fixture
def base_prefix():
    return ''


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'host': 'Target.Host',
        'access_key': 'Dont dead',
        'secret_key': 'open inside',
    }


@pytest.fixture
def settings():
    return {
        'bucket': 'that_kerning',
        'region': 'us-east-1',
        'encrypt_uploads': False
    }


@pytest.fixture
def mock_time(monkeypatch):
    mock_time_value = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time_value)
    
    fixed_datetime = datetime.datetime(2016, 2, 5, 15, 8, 50, tzinfo=datetime.timezone.utc)
    
    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz:
                return fixed_datetime
            return fixed_datetime.replace(tzinfo=None)
        
        @classmethod
        def utcnow(cls):
            return fixed_datetime.replace(tzinfo=None)
    
    monkeypatch.setattr(datetime, 'datetime', MockDateTime)


@pytest.fixture
def provider(auth, credentials, settings):
    return S3CompatSigV4Provider(auth, credentials, settings)


@pytest.fixture
def generate_url_helper(provider):
    """Generate presigned URLs for boto3-based S3CompatSigV4Provider."""
    def _generate_url(key=None, method='GET', expires=100, query_parameters=None,
                     response_headers=None, headers=None, encrypt_key=False):
        """Generate a presigned URL for S3CompatSigV4Provider."""
        method_upper = method.upper()
        
        if key:
            if method_upper == 'POST':
                if query_parameters and any(k.lower() == 'delete' for k in query_parameters.keys()):
                    client_method = 'delete_objects'
                elif query_parameters and 'uploads' in query_parameters:
                    client_method = 'create_multipart_upload'
                elif query_parameters and 'uploadId' in query_parameters:
                    client_method = 'complete_multipart_upload'
                else:
                    client_method = 'put_object'
            elif method_upper == 'DELETE':
                if query_parameters and 'uploadId' in query_parameters:
                    client_method = 'abort_multipart_upload'
                else:
                    client_method = 'delete_object'
            elif method_upper == 'GET':
                if query_parameters and 'uploadId' in query_parameters:
                    client_method = 'list_parts'
                else:
                    client_method = 'get_object'
            elif method_upper == 'PUT':
                if query_parameters and 'uploadId' in query_parameters and 'partNumber' in query_parameters:
                    client_method = 'upload_part'
                else:
                    client_method = 'put_object'
            else:
                method_map = {
                    'HEAD': 'head_object',
                }
                client_method = method_map.get(method_upper, 'get_object')
            params = {'Bucket': provider.bucket_name, 'Key': key}
        else:
            if query_parameters and 'versions' in query_parameters:
                client_method = 'list_object_versions'
            elif query_parameters and any(k.lower() == 'delete' for k in query_parameters.keys()):
                client_method = 'delete_objects'
            else:
                client_method = 'list_objects_v2'
            params = {'Bucket': provider.bucket_name}
        
        if query_parameters:
            for key_param, value_param in query_parameters.items():
                if key_param.lower() in ['versions', 'delete', 'uploads']:
                    continue
                if key_param == 'uploadId':
                    params['UploadId'] = value_param
                elif key_param == 'partNumber':
                    params['PartNumber'] = int(value_param)
                elif key_param in ['prefix', 'delimiter']:
                    params[key_param.capitalize()] = value_param
                elif key_param in ['Prefix', 'Delimiter', 'VersionIdMarker', 'KeyMarker', 'VersionId']:
                    params[key_param] = value_param
                else:
                    params[key_param] = value_param
        
        if response_headers:
            for rh_key, rh_value in response_headers.items():
                param_key = ''.join(word.capitalize() for word in rh_key.replace('response-', '').split('-'))
                param_key = 'Response' + param_key
                params[param_key] = rh_value
        
        if encrypt_key or headers:
            pass
        
        return provider.connection.generate_presigned_url(
            client_method, Params=params, ExpiresIn=expires, HttpMethod=method_upper
        )
    
    return _generate_url


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def file_header_metadata():
    return {
        'Content-Length': '9001',
        'Last-Modified': 'SomeTime',
        'Content-Type': 'binary/octet-stream',
        'Etag': '"fba9dede5f27731c9771645a39863328"',
        'x-amz-server-side-encryption': 'AES256'
    }


@pytest.fixture
def file_metadata_headers_object(file_header_metadata):
    return S3CompatSigV4FileMetadataHeaders('test-path', file_header_metadata)


@pytest.fixture
def file_metadata_object():
    content = OrderedDict(Key='my-image.jpg',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag="fba9dede5f27731c9771645a39863328",
                          Size='434234',
                          StorageClass='STANDARD')

    return S3CompatSigV4FileMetadata(content)


@pytest.fixture
def folder_key_metadata_object():
    content = OrderedDict(Key='naptime/folder/folder1',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag='"fba9dede5f27731c9771645a39863328"',
                          Size='0',
                          StorageClass='STANDARD')

    return S3CompatSigV4FolderKeyMetadata(content)


@pytest.fixture
def folder_metadata_object():
    content = OrderedDict(Prefix='photos/',
                          created_at='2009-10-12T17:50:30.000Z',
                          updated_at='2009-10-12T17:50:30.000Z')
    return S3CompatSigV4FolderMetadata(content)


@pytest.fixture
def revision_metadata_object():
    content = OrderedDict(
        Key='single-version.file',
        VersionId='3/L4kqtJl40Nr8X8gdRQBpUMLUo',
        IsLatest='true',
        LastModified='2009-10-12T17:50:30.000Z',
        ETag='"fba9dede5f27731c9771645a39863328"',
        Size=434234,
        StorageClass='STANDARD',
        Owner=OrderedDict(
            ID='75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a',
            DisplayName='mtd@amazon.com'
        )
    )

    return S3CompatSigV4Revision(content)


@pytest.fixture
def copy_object_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <CopyObjectResult>
        <ETag>string</ETag>
        <LastModified>timestamp</LastModified>
        <ChecksumCRC32>string</ChecksumCRC32>
        <ChecksumCRC32C>string</ChecksumCRC32C>
        <ChecksumSHA1>string</ChecksumSHA1>
        <ChecksumSHA256>string</ChecksumSHA256>
    </CopyObjectResult>'''


@pytest.fixture
def api_error_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>Internal Error</Code>
        <Message>Internal Error</Message>
        <Resource>/object/path</Resource>
        <RequestId>1234567890</RequestId>
    </Error>'''


@pytest.fixture
def single_version_metadata():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
        <Name>bucket</Name>
        <Prefix>my</Prefix>
        <KeyMarker/>
        <VersionIdMarker/>
        <MaxKeys>5</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Version>
            <Key>single-version.file</Key>
            <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
            <IsLatest>true</IsLatest>
            <LastModified>2009-10-12T17:50:30.000Z</LastModified>
            <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
            <Size>434234</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
    </ListVersionsResult>'''


@pytest.fixture
def version_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
        <Name>bucket</Name>
        <Prefix>my</Prefix>
        <KeyMarker/>
        <VersionIdMarker/>
        <MaxKeys>5</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
            <IsLatest>true</IsLatest>
            <LastModified>2009-10-12T17:50:30.000Z</LastModified>
            <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
            <Size>434234</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>QUpfdndhfd8438MNFDN93jdnJFkdmqnh893</VersionId>
            <IsLatest>false</IsLatest>
            <LastModified>2009-10-10T17:50:30.000Z</LastModified>
            <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>
            <Size>166434</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>UIORUnfndfhnw89493jJFJ</VersionId>
            <IsLatest>false</IsLatest>
            <LastModified>2009-10-11T12:50:30.000Z</LastModified>
            <ETag>&quot;772cf535f27731c974343645a3985328&quot;</ETag>
            <Size>64</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
    </ListVersionsResult>'''


@pytest.fixture
def folder_and_contents(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}thisfolder/</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>{prefix}thisfolder/item1</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>{prefix}thisfolder/item2</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_empty_metadata():
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>'''


@pytest.fixture
def folder_item_metadata(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}naptime/</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_metadata(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}my-image.jpg</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>434234</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>{prefix}my-third-image.jpg</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;1b2cf535f27731c974343645a3985328&quot;</ETag>
                <Size>64994</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <CommonPrefixes>
                <Prefix>{prefix}   photos/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_metadata_paginated(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>true</IsTruncated>
            <NextContinuationToken>token-for-next-page</NextContinuationToken>
            <Contents>
                <Key>{prefix}my-image.jpg</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>434234</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <CommonPrefixes>
                <Prefix>{prefix}   photos/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_single_item_metadata(base_prefix):
    return'''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Contents>
            <Key>{prefix}my-image.jpg</Key>
            <LastModified>2009-10-12T17:50:30.000Z</LastModified>
            <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
            <Size>434234</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Contents>
        <CommonPrefixes>
            <Prefix>{prefix}   photos/</Prefix>
        </CommonPrefixes>
    </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def complete_upload_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
        <Bucket>Example-Bucket</Bucket>
        <Key>Example-Object</Key>
        <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
    </CompleteMultipartUploadResult>'''


@pytest.fixture
def create_session_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Bucket>example-bucket</Bucket>
       <Key>example-object</Key>
       <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
    </InitiateMultipartUploadResult>'''


@pytest.fixture
def generic_http_403_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>AccessDenied</Code>
        <Message>Access Denied</Message>
        <RequestId>656c76696e6727732072657175657374</RequestId>
        <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
    </Error>'''


@pytest.fixture
def generic_http_404_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>NotFound</Code>
        <Message>Not Found</Message>
        <RequestId>656c76696e6727732072657175657374</RequestId>
        <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
    </Error>'''


@pytest.fixture
def list_parts_resp_empty():
    return '''<?xml version="1.0" encoding="UTF-8"?>
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
    </ListPartsResult>'''


@pytest.fixture
def list_parts_resp_not_empty():
    return '''<?xml version="1.0" encoding="UTF-8"?>
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
        <IsTruncated>true</IsTruncated>
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
    </ListPartsResult>'''


@pytest.fixture
def upload_parts_headers_list():
    return '''{
        "headers_list": [
            {
                "x-amz-id-2": "Vvag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==",
                "x-amz-request-id": "656c76696e6727732072657175657374",
                "Date": "Mon, 1 Nov 2010 20:34:54 GMT",
                "ETag": "b54357faf0632cce46e942fa68356b38",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            },
            {
                "x-amz-id-2": "imru9pO4ZVKnJ2Qz7Vvag1LuByRx9e6j5On/CAtRPfTaOFg1NPcfTW==",
                "x-amz-request-id": "732072657175657374656c76696e75657374",
                "Date": "Mon, 1 Nov 2010 20:35:55 GMT",
                "ETag": "46e942fa68356b38b54357faf0632cce",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            },
            {
                "x-amz-id-2": "yRx9e6j5Onimru9pOVvag1LuB4ZVKnJ2Qz7/cfTWAtRPf1NPTaOFg==",
                "x-amz-request-id": "67277320726571656c76696e75657374",
                "Date": "Mon, 1 Nov 2010 20:36:56 GMT",
                "ETag": "af0632cce46e942fab54357f68356b38",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            }
        ]
    }'''


def location_response(location):
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{location}</LocationConstraint>
    '''.format(location=location)


def list_objects_response(keys, truncated=False):
    response = '''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>'''

    response += '<IsTruncated>' + str(truncated).lower() + '</IsTruncated>'
    response += ''.join(map(
        lambda x: '<Contents><Key>{}</Key></Contents>'.format(x),
        keys
    ))

    response += '</ListBucketResult>'

    return response.encode('utf-8')


def bulk_delete_body(keys):
    payload = '<?xml version="1.0" encoding="UTF-8"?>'
    payload += '<Delete>'
    payload += ''.join(map(
        lambda x: '<Object><Key>{}</Key></Object>'.format(x),
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


def build_folder_params(path):
    prefix = path.full_path.lstrip('/')
    return {'prefix': prefix, 'delimiter': '/'}


def build_folder_params_with_max_key(path):
    return {'prefix': path.path, 'delimiter': '/', 'max-keys': '1000'}


def list_upload_chunks_body(parts_metadata):
    payload = '''<?xml version="1.0" encoding="UTF-8"?>
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
    '''.encode('utf-8')

    md5 = compute_md5(BytesIO(payload))

    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5[1],
        'Content-Type': 'text/xml',
    }

    return payload, headers


class TestProviderConstruction:

    def test_https(self, auth, credentials, settings):
        provider = S3CompatSigV4Provider(auth, {'host': 'securehost',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert provider.connection.use_ssl
        assert provider.connection.verify_ssl
        assert provider.connection.endpoint_url == 'https://securehost'

        provider = S3CompatSigV4Provider(auth, {'host': 'securehost:443',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert provider.connection.use_ssl
        assert provider.connection.verify_ssl
        assert provider.connection.endpoint_url == 'https://securehost'

    def test_http(self, auth, credentials, settings):
        provider = S3CompatSigV4Provider(auth, {'host': 'normalhost:80',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert not provider.connection.use_ssl
        assert provider.connection.endpoint_url == 'http://normalhost'

        provider = S3CompatSigV4Provider(auth, {'host': 'normalhost:8080',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert not provider.connection.use_ssl
        assert provider.connection.endpoint_url == 'http://normalhost:8080'


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_header_metadata, mock_time, generate_url_helper):
        file_path = 'foobah'
        full_path = file_path
        prefix = provider.prefix
        if prefix:
            full_path = prefix + full_path
        params_for_dir = {'Prefix': full_path + '/', 'Delimiter': '/'}
        good_metadata_url = generate_url_helper(key=full_path, method='HEAD', expires=100)
        bad_metadata_url = generate_url_helper(method='GET', expires=100, query_parameters=params_for_dir)
        aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_header_metadata)
        aiohttpretty.register_uri('GET', bad_metadata_url, status=404)

        assert WaterButlerPath('/') == await provider.validate_v1_path('/')

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time, generate_url_helper):
        folder_path = 'Photos'
        full_path = folder_path
        prefix = provider.prefix
        if prefix:
            full_path = prefix + full_path

        params_for_dir = {'Prefix': full_path + '/', 'Delimiter': '/'}
        good_metadata_url = generate_url_helper(method='GET', expires=100, query_parameters=params_for_dir)
        bad_metadata_url = generate_url_helper(key=full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri(
            'GET', good_metadata_url,
            body=folder_metadata, headers={'Content-Type': 'application/xml'}
        )
        aiohttpretty.register_uri('HEAD', bad_metadata_url, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_path + '/')

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
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_root(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, headers=file_header_metadata)

        response_headers = {'response-content-disposition':
                            'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'}
        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)

        aiohttpretty.register_uri('GET', get_url,
                              body=b'delicious',
                              headers=file_header_metadata,
                              auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'
        assert result._size == 9

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, headers=file_header_metadata)

        response_headers = {'response-content-disposition':
                            'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'}
        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', get_url,
                                  body=b'de', auto_length=True, status=206)

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        content_size = result._size
        assert content == b'de'
        assert content_size == 2
        assert aiohttpretty.has_call(method='GET', uri=get_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        versionid_parameter = {'VersionId': 'someversion'}

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, query_parameters=versionid_parameter)
        aiohttpretty.register_uri('HEAD', head_url, headers={'Content-Length': '9'})

        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, query_parameters=versionid_parameter, response_headers={'response-content-disposition': 'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'})
        aiohttpretty.register_uri('GET', get_url,
                                  body=b'delicious', auto_length=True)

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
    async def test_download_with_display_name(self, provider, mock_time, generate_url_helper, display_name_arg, expected_name):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, headers={'Content-Length': '9'})

        response_headers = {
            'response-content-disposition': ('attachment; filename="{}"; '
                                             'filename*=UTF-8\'\'{}').format(expected_name,
                                                                             expected_name)
        }
        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', get_url,
                                  body=b'delicious', auto_length=True)

        result = await provider.download(path, display_name=display_name_arg)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, status=404)

        response_headers = {'response-content-disposition':
                            'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'}
        url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_no_content_length(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, headers=file_header_metadata)

        no_content_length_metadata = file_header_metadata.copy()
        del no_content_length_metadata['Content-Length']

        response_headers = {'response-content-disposition':
                            'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'}
        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', get_url,
                                  body=b'delicious', headers=no_content_length_metadata)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'
        assert result._size == int(file_header_metadata['Content-Length'])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_content_replaced(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)

        head_header_metadata = file_header_metadata.copy()
        file_header_metadata['ETag'] = '"1accb31fcf202eba0c0f41fa2f09b4d7"'
        file_header_metadata['Content-Length'] = 300
        head_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', head_url, headers=head_header_metadata)

        response_headers = {'response-content-disposition':
                            'attachment; filename="muhtriangle"; filename*=UTF-8\'\'muhtriangle'}
        get_url = generate_url_helper(key=path.full_path, method='GET', expires=100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', get_url,
                                  body=b'delicious', headers=file_header_metadata, auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'
        assert result._size == 9
        assert aiohttpretty.has_call(method='HEAD', uri=head_url)
        assert aiohttpretty.has_call(method='GET', uri=get_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/', prepend=provider.prefix))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_content, file_stream, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100)
        metadata_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)
        header = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_encrypted(self, provider, file_content, file_stream, file_header_metadata, mock_time, generate_url_helper):
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100, encrypt_key=True)
        metadata_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100)
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        headers = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.extra['encryption'] == 'AES256'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_chunked(self, provider, file_stream, mock_time):
        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._chunked_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._chunked_upload.assert_called_with(file_stream, path)

        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete(self, provider, upload_parts_headers_list, file_stream, mock_time):
        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]

        provider.metadata = MockCoroutine()
        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.return_value = headers_list
        provider._complete_multipart_upload = MockCoroutine()

        await provider._chunked_upload(file_stream, path)

        provider._create_upload_session.assert_called_with(path)
        provider._upload_parts.assert_called_with(file_stream, path, upload_id)
        provider._complete_multipart_upload.assert_called_with(path, upload_id, headers_list)


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_aborted_success(self, provider, file_stream, mock_time):
        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.side_effect = Exception('error')
        provider._complete_multipart_upload = MockCoroutine()
        provider._abort_chunked_upload = MockCoroutine()
        provider._abort_chunked_upload.return_value = True

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)
        msg = 'An unexpected error has occurred during the multi-part upload.'
        assert str(exc.value) == ', '.join(['500', msg])

        provider._create_upload_session.assert_called_with(path)
        provider._upload_parts.assert_called_with(file_stream, path, upload_id)
        provider._abort_chunked_upload.assert_called_with(path, upload_id)
        provider._complete_multipart_upload.assert_not_called()


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('fails_at, expect_notice', [
        ('parts', False),
        ('commit-request', True),
        ('commit-read', True),
    ])
    async def test_chunked_upload_500_branch_notices_only_a_commit_failure(
            self, provider, file_stream, mock_time, fails_at, expect_notice):
        assert issubclass(asyncio.CancelledError, Exception)
        assert not issubclass(asyncio.CancelledError, pd_provider.CONNECTION_ERRORS)

        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        released = []

        class _AnswerWeCannotRead:
            """A commit answer whose body read is the part that gets cancelled."""

            async def read(self):
                raise asyncio.CancelledError('cancelled while reading the commit answer')

            async def release(self):
                released.append(True)

        if fails_at == 'parts':
            provider._upload_parts = MockCoroutine(
                side_effect=asyncio.CancelledError('cancelled during the parts'))
            provider._make_upload_request = MockCoroutine()
        else:
            provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"e"'}])
            if fails_at == 'commit-request':
                provider._make_upload_request = MockCoroutine(
                    side_effect=asyncio.CancelledError('cancelled while sending the commit'))
            else:
                provider._make_upload_request = MockCoroutine(
                    return_value=_AnswerWeCannotRead())

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert (provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message) is expect_notice
        if fails_at == 'parts':
            provider._make_upload_request.assert_not_called()
        else:
            assert provider._make_upload_request.call_count == 1
        assert released == ([True] if fails_at == 'commit-read' else [])


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_exception_from_response_contract_head(self, provider, mock_time,
                                                         generate_url_helper):
        # No body on HEAD -- _parse_s3_error_body must degrade to (None, None).
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='HEAD', expires=100,
                                  headers={}, query_parameters={})
        aiohttpretty.register_uri('HEAD', url, status=403)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider.make_request('HEAD', url, expects=(HTTPStatus.OK, ),
                                        throws=exceptions.UploadError)

        assert exc.value.data is None
        assert isinstance(exc.value.message, str)
        assert provider._parse_s3_error_body(exc.value) == (None, None)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_contiguous_upload_quota_exceeded_over_http(self, provider, file_stream,
                                                              mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100,
                                  headers={}, query_parameters={})
        error_body = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<Error><Code>QuotaExceeded</Code>'
                      '<Message>The bucket quota has been exceeded</Message>'
                      '<RequestId>REQ123</RequestId></Error>')
        aiohttpretty.register_uri('PUT', url, status=403, body=error_body,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._contiguous_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert exc.value.is_user_error
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert 'QuotaExceeded' in exc.value.message
        assert aiohttpretty.has_call(method='PUT', uri=url)


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('stage', ['contiguous', 'create-session', 'chunked'])
    async def test_translated_error_does_not_chain_the_raw_body(self, provider, file_stream,
                                                                mock_time, stage):
        # Translated error must suppress __context__ to keep raw body out of traceback/Sentry.
        import traceback

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Resource>/bucket/secret-key-name</Resource></Error>')
        failure = storage_error({'response': error_xml}, code=403)

        if stage == 'contiguous':
            provider.make_request = MockCoroutine(side_effect=failure)
            coro = provider._contiguous_upload(file_stream, path)
        else:
            assert file_stream.size == 6
            provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
            provider.CHUNK_SIZE = 2
            provider._abort_chunked_upload = MockCoroutine(return_value=True)
            if stage == 'create-session':
                provider.make_request = MockCoroutine(side_effect=failure)
            else:
                provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
                provider._upload_parts = MockCoroutine(side_effect=failure)
            coro = provider._chunked_upload(file_stream, path)

        with pytest.raises(exceptions.UploadError) as exc:
            await coro

        assert exc.value.__suppress_context__ is True
        rendered = ''.join(traceback.format_exception(type(exc.value), exc.value,
                                                      exc.value.__traceback__))
        assert 'secret-key-name' not in rendered

    def test_translate_upload_error_logs_raw_body(self, provider, caplog):
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>AccessDenied</Code><Message>Access Denied</Message>'
                     '<RequestId>TESTREQUESTID</RequestId>'
                     '<Resource>/bucket/foobah</Resource></Error>')
        err = storage_error({'response': error_xml}, code=HTTPStatus.FORBIDDEN)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            provider._translate_upload_error(err)

        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 1
        logged = records[0].getMessage()
        assert 'TESTREQUESTID' in logged
        assert '/bucket/foobah' in logged
        assert 'AccessDenied' in logged
        assert str(int(HTTPStatus.FORBIDDEN)) in logged
        assert records[0].levelno == logging.ERROR


    @pytest.mark.parametrize('body', [
        b'\xff' * 512,
        b'\xff' * 4096,
        '\u3042' * 512,
        ('\u3042' * 512).encode('utf-8'),
        b'<html>' + b'\xc3\x28' * 300 + '\u3042'.encode('utf-8') * 100,
        b'<Error><Code>AccessDenied</Code></Error>',
        '<Error><Code>AccessDenied</Code></Error>',
        b'',
        '',
    ])
    def test_bounded_body_never_exceeds_the_declared_limit(self, body):
        # Weigh the return value in UTF-8: bounding the log line would miss 3x inflate on decode.
        bounded = pd_provider._bounded_body(body)

        assert len(bounded.encode('utf-8')) <= pd_provider.ERROR_BODY_LOG_LIMIT
        source = body if isinstance(body, bytes) else body.encode('utf-8')
        decoded = source.decode('utf-8', 'replace')
        if len(decoded.encode('utf-8')) <= pd_provider.ERROR_BODY_LOG_LIMIT:
            assert bounded == decoded
        else:
            assert bounded
            assert decoded.startswith(bounded)
            assert len(bounded.encode('utf-8')) > pd_provider.ERROR_BODY_LOG_LIMIT // 3

    @pytest.mark.parametrize('body, expected', [
        (b'\xff' * 512, '\ufffd' * 170),
        (b'\xff' * 4096, '\ufffd' * 170),
        ('\u3042' * 512, '\u3042' * 170),
        (('\u3042' * 512).encode('utf-8'), '\u3042' * 170),
    ])
    def test_bounded_body_keeps_exactly_the_leading_bytes(self, body, expected):
        assert pd_provider.ERROR_BODY_LOG_LIMIT == 512
        assert pd_provider._bounded_body(body) == expected

    def test_bounded_body_passes_none_through(self):
        assert pd_provider._bounded_body(None) is None


    def test_check_for_200_error_preserves_error_body(self, provider):
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        with pytest.raises(exceptions.UploadError) as exc:
            provider._check_for_200_error(error_xml.encode('utf-8'),
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)

        assert provider._parse_s3_error_body(exc.value)[0] == 'QuotaExceeded'
        assert provider._translate_upload_error(exc.value).code == \
            HTTPStatus.INSUFFICIENT_STORAGE


    def test_check_for_200_error_malformed_xml_is_controlled(self, provider):
        with pytest.raises(exceptions.UploadError) as exc:
            provider._check_for_200_error(b'<Error><Code>QuotaExceeded',
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)

        assert int(exc.value.code) == int(HTTPStatus.BAD_GATEWAY)


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_session_error_keeps_waterbutler_message(
            self, provider, file_stream, mock_time):
        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=b'this is not the expected xml')
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert 'stale multipart upload session' in exc.value.message
        assert provider.UNCLASSIFIED_STORAGE_ERROR_MESSAGE not in exc.value.message


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('error_body', [
        b'<Error/>',
        b'<Error><Message>no code here</Message></Error>',
        b'<Error><Code>QuotaExceeded',
    ])
    async def test_chunked_upload_complete_unclassifiable_warns_upload_may_exist(
            self, provider, file_stream, mock_time, error_body):
        # Fail-closed: complete may have succeeded; telling user only "upload failed" invites a duplicate.
        set_chunked_limits(provider, file_stream)

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=error_body)
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message
        assert 'may in fact have completed' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        assert 'check the file list' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        assert 'Error' not in exc.value.message


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport,error_code,expect_notice', [
        ('direct_4xx', 'QuotaExceeded', False),
        ('direct_4xx', 'AccessDenied', False),
        ('direct_4xx', 'InternalError', True),
        ('direct_5xx', 'QuotaExceeded', False),
        ('direct_5xx', 'AccessDenied', False),
        ('direct_5xx', 'InternalError', True),
        ('complete_200_error', 'QuotaExceeded', False),
        ('complete_200_error', 'AccessDenied', False),
        ('complete_200_error', 'InternalError', True),
        ('disconnect', 'AccessDenied', True),
        ('disconnect', 'InternalError', True),
        ('disconnect', 'QuotaExceeded', True),
        ('broken_xml', 'AccessDenied', True),
        ('broken_xml', 'InternalError', True),
        ('broken_xml', 'QuotaExceeded', True),
    ])
    async def test_commit_notice_5_paths_3_classifications(
            self, provider, file_stream, mock_time, transport, error_code,
            expect_notice):
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        arrange_commit_failure(provider, transport, error_code)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert (provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message) is expect_notice
        if not expect_notice and error_code in pd_settings.QUOTA_EXCEEDED_ERROR_CODES:
            assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
            assert len(provider.QUOTA_EXCEEDED_MESSAGE) > 10
            assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_quota_507_without_a_body_suppresses_the_notice(self, provider, file_stream,
                                                                  mock_time):
        # HTTP 507 is treated as quota exhaustion regardless of body; the table does not cover this input.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider.make_request = MockCoroutine(
            side_effect=exceptions.UploadError('no body', code=507))

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message


    @pytest.mark.parametrize('error_code', DEFINITIVE_REJECTION_CODES)
    def test_every_definitive_rejection_code_suppresses_the_notice(self, provider, error_code):
        assert pd_provider.DEFINITIVE_REJECTION_CODES == frozenset(DEFINITIVE_REJECTION_CODES)
        err = pd_provider._mark_commit_outcome_unknown(pd_provider._mark_storage_response(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=400)))
        assert provider._commit_outcome_note(err) == ''


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('chunked', [False, True])
    async def test_connection_error_log_does_not_leak_the_signature(
            self, provider, file_stream, mock_time, caplog, chunked):
        # aiohttp embeds the presigned URL in ClientOSError; the signature must not reach the log.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5 if chunked else 4096
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        presigned = ('https://minio.example/bkt/key?X-Amz-Algorithm=AWS4-HMAC-SHA256'
                     '&X-Amz-Credential=AKIAEXAMPLE%2F20260913%2Fus-east-1%2Fs3%2Faws4_request'
                     '&X-Amz-Signature=1f2e3d4c5b6a7988SECRETSIG')
        err = aiohttp.ClientOSError(32,
                                    'Can not write request body for {}'.format(presigned))
        provider.make_request = MockCoroutine(side_effect=err)

        with caplog.at_level(logging.DEBUG, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.UploadError) as exc:
                if chunked:
                    await provider._chunked_upload(file_stream, path)
                else:
                    await provider._contiguous_upload(file_stream, path)

        logged = '\n'.join(r.getMessage() for r in caplog.records if r.name == PROVIDER_LOGGER)
        assert 'X-Amz-Signature' not in logged
        assert 'X-Amz-Credential' not in logged
        assert 'SECRETSIG' not in logged
        assert 'ClientOSError' in logged
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('failure, expected_code', [
        ('connection', HTTPStatus.BAD_GATEWAY),
        ('unexpected', HTTPStatus.INTERNAL_SERVER_ERROR),
    ])
    async def test_commit_failure_does_not_chain_the_presigned_url(
            self, provider, file_stream, mock_time, failure, expected_code):
        # Pin __suppress_context__ on the two from-None sites not reached by the session-creation test.
        presigned = ('https://minio.example/bkt/key?X-Amz-Algorithm=AWS4-HMAC-SHA256'
                     '&X-Amz-Credential=AKIAEXAMPLE%2F20260913%2Fus-east-1%2Fs3%2Faws4_request'
                     '&X-Amz-Signature=1f2e3d4c5b6a7988SECRETSIG')
        if failure == 'connection':
            err = aiohttp.ClientOSError(
                32, 'Can not write request body for {}'.format(presigned))
        else:
            err = ValueError('unexpected failure while committing to {}'.format(presigned))

        arrange_chunked_commit(provider)
        provider._make_upload_request = MockCoroutine(side_effect=err)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(
                file_stream, WaterButlerPath('/foobah', prepend=provider.prefix))

        assert exc.value.code == expected_code
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True
        assert 'SECRETSIG' not in str(exc.value.message)
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message


    def test_parse_s3_error_body_empty_code_element(self, provider):
        # Empty <Code/> becomes None; attributes-only becomes dict; neither is a usable code.
        err = exceptions.UploadError(
            {'response': '<Error><Code/><Message>nope</Message></Error>'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)

        err = exceptions.UploadError(
            {'response': '<Error><Code lang="en"/></Error>'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)


    def test_parse_s3_error_body_logs_xml_parse_failure(self, provider, caplog):
        err = storage_error({'response': 'not xml at all'}, code=500)
        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            provider._parse_s3_error_body(err)
        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 1
        msg = records[0].getMessage()
        assert 'ExpatError' in msg
        assert 'xml_parse_failure' in msg


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('upload_id_xml', [
        '<UploadId/>',
        '<UploadId>   </UploadId>',
        '<UploadId attr="x"/>',
    ])
    async def test_create_upload_session_blank_upload_id(self, provider, mock_time,
                                                         upload_id_xml):
        # Unusable UploadId must not be returned; every later request would be signed with None.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<InitiateMultipartUploadResult>'
                '{}'
                '</InitiateMultipartUploadResult>').format(upload_id_xml)

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body.encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._create_upload_session(path)

        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert 'unexpected response' in exc.value.message


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_multipart_upload_releases_when_read_fails(self, provider, mock_time):
        # read() was outside the try; a connection dropped mid-body skipped finally and leaked the connection.
        import aiohttp

        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        resp = mock.Mock()
        resp.read = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(aiohttp.ServerDisconnectedError):
            await provider._complete_multipart_upload(path, 'EXAMPLEUPLOADID',
                                                      [{'ETAG': '"etag1"'}])

        assert resp.release.call_count == 1


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_upload_session_releases_when_read_fails(self, provider, mock_time):
        # Same connection-leak defect as _complete_multipart_upload; read() must be inside try/finally.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        resp = mock.Mock()
        resp.read = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(aiohttp.ServerDisconnectedError):
            await provider._create_upload_session(path)

        assert resp.release.call_count == 1


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_contiguous(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 10
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._contiguous_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._contiguous_upload.assert_called_with(file_stream, path)

        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_no_encryption(self, provider, create_session_resp, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        init_url = generate_url_helper(key=path.full_path, method='POST', expires=200, query_parameters={'uploads': ''})

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200)

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
                                                                        mock_time, generate_url_helper):
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        init_url = generate_url_helper(key=path.full_path, method='POST', expires=200, query_parameters={'uploads': ''}, encrypt_key=True)

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_with_full_path(self, provider,
                                                                        create_session_resp,
                                                                        mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix + 'project_folder/')
        init_url_full_path = generate_url_helper(key=path.full_path, method='POST', expires=200, query_parameters={'uploads': ''})
        init_url_path = generate_url_helper(key=path.path, method='POST', expires=200, query_parameters={'uploads': ''})

        aiohttpretty.register_uri('POST', init_url_full_path, body=create_session_resp, status=200)
        aiohttpretty.register_uri('POST', init_url_path, body=create_session_resp, status=200)

        session_id = await provider._create_upload_session(path)

        assert aiohttpretty.has_call(method='POST', uri=init_url_full_path)
        assert aiohttpretty.has_call(method='POST', uri=init_url_path) is False
        assert session_id is not None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts(self, provider, file_stream,
                                               upload_parts_headers_list):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
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
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
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
                                              mock_time, generate_url_helper):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {
            'partNumber': str(chunk_number),
            'uploadId': upload_id,
        }
        headers = {'Content-Length': str(provider.CHUNK_SIZE)}
        upload_part_url = generate_url_helper(key=path.full_path, method='PUT', expires=200, query_parameters=params, headers=headers)
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}
        aiohttpretty.register_uri('PUT', upload_part_url, status=200, headers=part_headers)

        part_metadata = await provider._upload_part(file_stream, path, upload_id, chunk_number,
                                                    provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url)
        assert part_headers == part_metadata

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_part_with_full_path(self, provider, file_stream,
                                              upload_parts_headers_list,
                                              mock_time, generate_url_helper):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix + 'project_folder/')
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u'
        params = {
            'partNumber': str(chunk_number),
            'uploadId': upload_id,
        }
        headers = {'Content-Length': str(provider.CHUNK_SIZE)}
        upload_part_url_full_path = generate_url_helper(key=path.full_path, method='PUT', expires=200, query_parameters=params, headers=headers)
        upload_part_url_path = generate_url_helper(key=path.path, method='PUT', expires=200, query_parameters=params, headers=headers)
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}

        aiohttpretty.register_uri('PUT', upload_part_url_path, status=200, headers=part_headers)
        aiohttpretty.register_uri('PUT', upload_part_url_full_path, status=200, headers=part_headers)

        part_metadata = await provider._upload_part(file_stream, path, upload_id, chunk_number,
                                                    provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url_full_path)
        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url_path) is False
        assert part_headers == part_metadata

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload(self, provider,
                                                            upload_parts_headers_list,
                                                            complete_upload_resp, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += '<PartNumber>{}</PartNumber>'.format(i+1)  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }

        complete_url = generate_url_helper(key=path.full_path, method='POST', expires=200, headers=headers, query_parameters=params)

        aiohttpretty.register_uri(
            'POST',
            complete_url,
            status=200,
            body=complete_upload_resp
        )

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload_with_full_path(self, provider,
                                                            upload_parts_headers_list,
                                                            complete_upload_resp, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix + 'project_folder/')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += '<PartNumber>{}</PartNumber>'.format(i+1)  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }

        complete_url_full_path = generate_url_helper(key=path.full_path, method='POST', expires=200, headers=headers, query_parameters=params)
        complete_url_path = generate_url_helper(key=path.path, method='POST', expires=200, headers=headers, query_parameters=params)

        aiohttpretty.register_uri(
            'POST',
            complete_url_full_path,
            status=200,
            body=complete_upload_resp
        )
        aiohttpretty.register_uri(
            'POST',
            complete_url_path,
            status=200,
            body=complete_upload_resp
        )

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url_full_path, params=params)
        assert aiohttpretty.has_call(method='POST', uri=complete_url_path, params=params) is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload_error(self, provider,
                                                            upload_parts_headers_list,
                                                            api_error_resp, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += '<PartNumber>{}</PartNumber>'.format(i+1)  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }

        complete_url = generate_url_helper(key=path.full_path, method='POST', expires=200, headers=headers, query_parameters=params)

        aiohttpretty.register_uri(
            'POST',
            complete_url,
            status=200,
            body=api_error_resp
        )

        with pytest.raises(exceptions.UploadError):
            await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('status', [408, 502, 503, 504])
    async def test_complete_multipart_upload_is_sent_exactly_once(
            self, provider, upload_parts_headers_list, mock_time, generate_url_helper, status):
        # retry=0: resend meets consumed UploadId; count POSTs over real HTTP to pin it.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]

        payload = '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>'
        for i, part in enumerate(headers_list):
            payload += '<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>'.format(
                i + 1, xml.sax.saxutils.escape(part['ETAG']))
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')
        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }

        complete_url = generate_url_helper(key=path.full_path, method='POST', expires=200,
                                           headers=headers, query_parameters=params)
        error_body = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<Error><Code>SlowDown</Code>'
                      '<Message>Please reduce your request rate.</Message></Error>')
        aiohttpretty.register_uri('POST', complete_url, status=status,
                                  body=error_body.encode('utf-8'))

        with pytest.raises(exceptions.UploadError):
            await provider._complete_multipart_upload(path, upload_id, headers_list)

        # Pin the retried statuses so widening core retry_on reports this parameter set as stale.
        assert provider._retry_on == {408, 502, 503, 504}
        assert status in provider._retry_on
        assert len(aiohttpretty.calls) == 1

    @pytest.mark.asyncio
    async def test_commit_post_is_sent_exactly_once(
            self, provider, file_stream, mock_time):
        calls = []

        async def first(request):
            await request.read()
            calls.append(request.path)
            raise web.HTTPTemporaryRedirect(location='/second')

        async def second(request):
            await request.read()
            calls.append(request.path)
            return web.Response(
                status=403, content_type='application/xml',
                text='<?xml version="1.0" encoding="UTF-8"?><Error>'
                     '<Code>SignatureDoesNotMatch</Code></Error>')

        app = web.Application()
        app.router.add_post('/first', first)
        app.router.add_post('/second', second)

        async with commit_server(provider, app) as server:
            arrange_chunked_commit(provider)
            with mock.patch.object(provider.connection, 'generate_presigned_url',
                                   return_value=server.url):
                with pytest.raises(exceptions.UploadError):
                    await provider._chunked_upload(
                        file_stream, WaterButlerPath('/foobah', prepend=provider.prefix))

        assert calls == ['/first']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_session_deleted(self, provider, generic_http_404_resp,
                                                        mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100, headers={}, query_parameters=params)
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_no_such_upload_is_already_clean(
            self, provider, mock_time, generate_url_helper):
        # Session is gone, which is the target state; retrying until cap would mislead the user.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100,
                                        headers={}, query_parameters=params)
        no_such_upload = ('<?xml version="1.0" encoding="UTF-8"?>'
                          '<Error><Code>NoSuchUpload</Code>'
                          '<Message>The specified upload does not exist.</Message></Error>')
        aiohttpretty.register_uri('DELETE', abort_url, body=no_such_upload.encode('utf-8'),
                                  status=404)

        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100,
                                       headers={}, query_parameters=params)
        aiohttpretty.register_uri('GET', list_url, body=no_such_upload.encode('utf-8'),
                                  status=404)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aborted is True
        # Decision(B): ListParts confirms the abort; one extra request, no retry budget burned.
        assert len(aiohttpretty.calls) == 2

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_is_fail_closed(self, provider, mock_time):
        # Confirmation exists because NoSuchUpload on DELETE does not prove parts are gone.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._list_uploaded_chunks = MockCoroutine(side_effect=Exception('boom'))

        assert await provider._abort_confirmed_by_list_parts(path, 'EXAMPLEUPLOADID') is False


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_does_not_use_the_retry_budget(self, provider, mock_time):
        # make_request retries 408/502/503/504 twice with 2s+4s sleep; confirmation must not stall the response.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._list_uploaded_chunks = MockCoroutine(side_effect=Exception('boom'))

        await provider._abort_confirmed_by_list_parts(path, 'EXAMPLEUPLOADID')

        assert provider._list_uploaded_chunks.call_args[1]['retry'] == 0


    @pytest.mark.parametrize('body,expected', [
        ('<ListPartsResult></ListPartsResult>', True),
        ('<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>', True),
        # Truncated listing with no Part element says nothing about later pages; false positive suppresses the manual-cleanup warning.
        ('<ListPartsResult><IsTruncated>true</IsTruncated></ListPartsResult>', False),
        ('<ListPartsResult><IsTruncated>true</IsTruncated>'
         '<Part><PartNumber>1</PartNumber></Part></ListPartsResult>', False),
    ])
    def test_no_parts_left_respects_is_truncated(self, provider, body, expected):
        assert provider._no_parts_left(
            ('<?xml version="1.0" encoding="UTF-8"?>' + body).encode('utf-8')) is expected


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_empty(self, provider, list_parts_resp_empty,
                                                   mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100, headers={}, query_parameters=params)
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_not_empty(self, provider, list_parts_resp_not_empty, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100, headers={}, query_parameters=params)
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_exception(self, provider, upload_parts_headers_list, file_stream, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100, headers={}, query_parameters=params)
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)
        provider._list_uploaded_chunks = MockCoroutine()
        provider._list_uploaded_chunks.side_effect = Exception('error')

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False
        provider._list_uploaded_chunks.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_with_full_path(self, provider, list_parts_resp_empty,
                                                   mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix + 'project_folder/')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        abort_url_full_path = generate_url_helper(key=path.full_path, method='DELETE', expires=100, headers={}, query_parameters=params)
        abort_url_path = generate_url_helper(key=path.path, method='DELETE', expires=100, headers={}, query_parameters=params)
        list_url_full_path = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        list_url_path = generate_url_helper(key=path.path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('DELETE', abort_url_full_path, status=204)
        aiohttpretty.register_uri('GET', list_url_full_path, body=list_parts_resp_empty, status=200)
        aiohttpretty.register_uri('DELETE', abort_url_path, status=204)
        aiohttpretty.register_uri('GET', list_url_path, body=list_parts_resp_empty, status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url_full_path)
        assert aiohttpretty.has_call(method='GET', uri=list_url_full_path)
        assert aiohttpretty.has_call(method='DELETE', uri=abort_url_path) is False
        assert aiohttpretty.has_call(method='GET', uri=list_url_path) is False
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_session_not_found(self,
                                                          provider,
                                                          generic_http_404_resp,
                                                          mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters=params)
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_empty_list(self,
                                                   provider,
                                                   list_parts_resp_empty,
                                                   mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters={'uploadId': upload_id})
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters={'uploadId': upload_id})
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_with_full_path(self,
                                                   provider,
                                                   list_parts_resp_empty,
                                                   mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix + 'project_folder/')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url_full_path = generate_url_helper(key=path.full_path, method='GET', expires=100, headers={}, query_parameters={'uploadId': upload_id})
        list_url_path = generate_url_helper(key=path.path, method='GET', expires=100, headers={}, query_parameters={'uploadId': upload_id})
        aiohttpretty.register_uri('GET', list_url_full_path, body=list_parts_resp_empty, status=200)
        aiohttpretty.register_uri('GET', list_url_path, body=list_parts_resp_empty, status=200)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url_full_path)
        assert aiohttpretty.has_call(method='GET', uri=list_url_path) is False
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/some-file', prepend=provider.prefix)

        query_params = {
            'Prefix': path.path.lstrip('/'),
            'Delimiter': '/',
            'VersionIdMarker': ''
        }
        versions_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params})
        params = {
            'prefix': path.path.lstrip('/'),
            'delimiter': '/',
            'version-id-marker': '',
            'versions': ''
        }
        version_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
                <Name>bucket</Name>
                <Prefix>some-file</Prefix>
                <KeyMarker/>
                <VersionIdMarker/>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <Version>
                    <Key>some-file</Key>
                    <VersionId>null</VersionId>
                    <IsLatest>true</IsLatest>
                    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                    <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
                    <Size>0</Size>
                    <Owner>
                        <ID>minio</ID>
                        <DisplayName>minio</DisplayName>
                    </Owner>
                    <StorageClass>STANDARD</StorageClass>
                </Version>
            </ListVersionsResult>'''
        aiohttpretty.register_uri('GET', versions_url, params=params, status=200, body=version_body)

        mock_delete_response = {
            'Deleted': [{'Key': 'some-file', 'VersionId': 'null'}],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)
        provider.bucket.delete_objects.assert_called_once()
        call_args = provider.bucket.delete_objects.call_args
        assert call_args[1]['Delete']['Objects'] == [{'Key': path.full_path, 'VersionId': 'null'}]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_confirm_delete(self, provider, version_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/')

        query_params_file = {
            'Prefix': '',
            'Delimiter': '/',
            'VersionIdMarker': ''
        }
        versions_url_file = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params_file})
        params_file = {'prefix': '', 'delimiter': '/', 'version-id-marker': '', 'versions': ''}
        
        query_params_folder = {'Prefix': ''}
        versions_url_folder = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params_folder})
        params_folder = {'prefix': '', 'versions': ''}
        
        aiohttpretty.register_uri(
            'GET',
            versions_url_file,
            params=params_file,
            body=version_metadata,
            status=200
        )
        aiohttpretty.register_uri(
            'GET',
            versions_url_folder,
            params=params_folder,
            body=version_metadata,
            status=200
        )

        prefix_check_query = {
            'Prefix': '',
            'Delimiter': '/'
        }
        prefix_check_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=prefix_check_query)
        prefix_check_params = {'prefix': '', 'delimiter': '/'}
        prefix_check_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>'''
        aiohttpretty.register_uri('GET', prefix_check_url, params=prefix_check_params,
                                body=prefix_check_body, status=200)

        mock_delete_response = {
            'Deleted': [
                {'Key': 'my-image.jpg', 'VersionId': '3/L4kqtJl40Nr8X8gdRQBpUMLUo'},
                {'Key': 'my-image.jpg', 'VersionId': 'QUpfdndhfd8438MNFDN93jdnJFkdmqnh893'},
                {'Key': 'my-image.jpg', 'VersionId': 'UIORUnfndfhnw89493jJFJ'}
            ],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        with pytest.raises(exceptions.DeleteError):
            await provider.delete(path)

        await provider.delete(path, confirm_delete=1)

        assert provider.bucket.delete_objects.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_with_versions(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/folder-to-delete/')

        query_params = {'Prefix': path.path}
        versions_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params})
        params = {'prefix': path.path, 'versions': ''}

        list_versions_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult>
                <Version>
                    <Key>folder-to-delete/file1.txt</Key>
                    <VersionId>111</VersionId>
                </Version>
                <Version>
                    <Key>folder-to-delete/file1.txt</Key>
                    <VersionId>222</VersionId>
                </Version>
                <DeleteMarker>
                    <Key>folder-to-delete/file2.txt</Key>
                    <VersionId>333</VersionId>
                </DeleteMarker>
            </ListVersionsResult>'''

        aiohttpretty.register_uri('GET', versions_url, params=params, body=list_versions_body, status=200)

        mock_delete_response = {
            'Deleted': [
                {'Key': 'folder-to-delete/file1.txt', 'VersionId': '111'},
                {'Key': 'folder-to-delete/file1.txt', 'VersionId': '222'},
                {'Key': 'folder-to-delete/file2.txt', 'VersionId': '333'}
            ],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        prefix_check_query = {
            'Prefix': 'folder-to-delete',
            'Delimiter': '/'
        }
        prefix_check_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=prefix_check_query)
        prefix_check_params = {'prefix': 'folder-to-delete', 'delimiter': '/'}
        prefix_check_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>'''
        aiohttpretty.register_uri('GET', prefix_check_url, params=prefix_check_params,
                                body=prefix_check_body, status=200)

        await provider._delete_folder(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)

        provider.bucket.delete_objects.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_truncated_response(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/large-folder/')

        query_params = {'Prefix': path.path}
        versions_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params})
        params1 = {'prefix': path.path, 'versions': ''}

        list_versions_body1 = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult>
                <IsTruncated>true</IsTruncated>
                <NextKeyMarker>large-folder/file2.txt</NextKeyMarker>
                <NextVersionIdMarker>222</NextVersionIdMarker>
                <Version>
                    <Key>large-folder/file1.txt</Key>
                    <VersionId>111</VersionId>
                </Version>
            </ListVersionsResult>'''

        aiohttpretty.register_uri('GET', versions_url, params=params1, body=list_versions_body1, status=200)

        query_params2 = {
            'Prefix': path.path,
            'KeyMarker': 'large-folder/file2.txt',
            'VersionIdMarker': '222'
        }
        versions_url2 = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params2})
        params2 = {
            'prefix': path.path,
            'versions': '',
            'key-marker': 'large-folder/file2.txt',
            'version-id-marker': '222'
        }

        list_versions_body2 = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult>
                <IsTruncated>false</IsTruncated>
                <Version>
                    <Key>large-folder/file2.txt</Key>
                    <VersionId>222</VersionId>
                </Version>
            </ListVersionsResult>'''

        aiohttpretty.register_uri('GET', versions_url2, params=params2, body=list_versions_body2, status=200)

        mock_delete_response = {
            'Deleted': [
                {'Key': 'large-folder/file1.txt', 'VersionId': '111'},
                {'Key': 'large-folder/file2.txt', 'VersionId': '222'}
            ],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        prefix_check_query = {
            'Prefix': 'large-folder',
            'Delimiter': '/'
        }
        prefix_check_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=prefix_check_query)
        prefix_check_params = {'prefix': 'large-folder', 'delimiter': '/'}
        prefix_check_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>'''
        aiohttpretty.register_uri('GET', prefix_check_url, params=prefix_check_params,
                                body=prefix_check_body, status=200)

        await provider._delete_folder(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params1)
        assert aiohttpretty.has_call(method='GET', uri=versions_url2, params=params2)

        provider.bucket.delete_objects.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_not_found(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/not-found-folder/')
        prefix = path.full_path.lstrip('/')  # 'not-found-folder/'

        query_params = {'Prefix': prefix}
        versions_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params})
        versions_params = {'prefix': prefix, 'versions': ''}
        list_versions_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult>
                <IsTruncated>false</IsTruncated>
            </ListVersionsResult>'''
        aiohttpretty.register_uri('GET', versions_url, params=versions_params,
                                body=list_versions_body, status=200)

        with pytest.raises(exceptions.NotFoundError):
            await provider._delete_folder(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=versions_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_delete_error(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/error-folder/')

        query_params = {'Prefix': path.path}
        versions_url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params})
        params = {'prefix': path.path, 'versions': ''}

        list_versions_body = '''<?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult>
                <Version>
                    <Key>error-folder/file1.txt</Key>
                    <VersionId>111</VersionId>
                </Version>
            </ListVersionsResult>'''

        aiohttpretty.register_uri('GET', versions_url, params=params, body=list_versions_body, status=200)

        mock_delete_response = {
            'Deleted': [],
            'Errors': [
                {
                    'Key': 'error-folder/file1.txt',
                    'VersionId': '111',
                    'Code': 'AccessDenied',
                    'Message': 'Access Denied'
                }
            ]
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        with pytest.raises(exceptions.DeleteError):
            await provider._delete_folder(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)
        provider.bucket.delete_objects.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_objects_batch_logs_partial_failure(self, provider, caplog):
        objects = [{'Key': 'f{}'.format(i)} for i in range(3)]
        provider.bucket.delete_objects = mock.Mock(return_value={
            'Deleted': [{'Key': 'f0'}],
            'Errors': [
                {'Key': 'f1', 'Code': 'AccessDenied'},
                {'Key': 'f2', 'Code': 'InternalError'},
            ],
        })
        with caplog.at_level(logging.ERROR, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.DeleteError):
                await provider._delete_objects_batch(objects, 'test')
        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 1
        msg = records[0].getMessage()
        assert '(test)' in msg
        assert 'total=3' in msg
        assert 'failed=2' in msg
        assert 'f1' in msg
        assert 'AccessDenied' in msg

    @pytest.mark.asyncio
    async def test_delete_objects_batch_limits_logged_keys_to_five(self, provider, caplog):
        objects = [{'Key': 'k{}'.format(i)} for i in range(8)]
        provider.bucket.delete_objects = mock.Mock(return_value={
            'Deleted': [],
            'Errors': [{'Key': 'k{}'.format(i), 'Code': 'X'} for i in range(8)],
        })
        with caplog.at_level(logging.ERROR, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.DeleteError):
                await provider._delete_objects_batch(objects, 'test')
        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        msg = records[-1].getMessage()
        assert 'k4' in msg
        assert 'k5' not in msg


    @pytest.mark.asyncio
    async def test_delete_objects_batch_wraps_botocore_error(self, provider, caplog):
        from botocore.exceptions import BotoCoreError
        provider.bucket.delete_objects = mock.Mock(side_effect=BotoCoreError())
        with caplog.at_level(logging.ERROR, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.DeleteError) as exc_info:
                await provider._delete_objects_batch([{'Key': 'x'}], 'test')
        assert 'BotoCoreError' in str(exc_info.value)
        assert exc_info.value.__suppress_context__
        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 1
        assert 'botocore error' in records[0].getMessage()


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_handle_data(self, provider):
        data = ['txt001.txt', 'abc']
        result, token = provider.handle_data(data)
        assert compare_digest(token, 'abc')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/darp/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_empty_next_token_ignored(self, provider, folder_metadata, mock_time, generate_url_helper):
        """Empty next_token should not send ContinuationToken to S3,
        preventing InvalidArgument errors from the storage backend."""
        path = WaterButlerPath('/darp/')
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url',
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url',
        }

        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path, revision=None, next_token='')

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_with_valid_continuation_token(self, provider, folder_metadata_paginated, mock_time, generate_url_helper):
        """Valid next_token should be sent as ContinuationToken and the response
        with NextContinuationToken should be handled correctly."""
        path = WaterButlerPath('/darp/')
        token = 'abc123-valid-token'
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url',
            'ContinuationToken': token,
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url',
            'continuation-token': token,
        }

        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata_paginated,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider._metadata_folder(path, next_token=token)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2] == 'token-for-next-page'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_self_listing(self, provider, folder_and_contents, mock_time, generate_url_helper):
        path = WaterButlerPath('/thisfolder/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, body=folder_and_contents)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result[:-1]:
            assert fobj.name != path.full_path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_just_a_folder_metadata_folder(self, provider, folder_item_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, body=folder_item_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/this-is-not-the-root/', prepend=provider.prefix)
        metadata_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})

        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, body=folder_empty_metadata,
                                  headers={'Content-Type': 'application/xml'})

        aiohttpretty.register_uri('HEAD', metadata_url, header=folder_empty_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == '/' + path.path
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_lastest_revision(self, provider, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == '/' + path.path
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/notfound.txt', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_content, file_stream, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100, headers={}, query_parameters={})
        metadata_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        headers = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers),

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, provider, file_stream, file_header_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100, headers={}, query_parameters={})
        metadata_url = generate_url_helper(key=path.full_path, method='HEAD', expires=100, headers={}, query_parameters={})
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )

        error_body = '''<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>InvalidDigest</Code>
            <Message>The Content-Md5 you specified is not valid.</Message>
        </Error>'''

        aiohttpretty.register_uri('PUT', url, status=400, body=error_body)

        with pytest.raises(exceptions.UploadError):
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raise_409(self, provider, folder_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/alreadyexists/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "alreadyexists", because a file or folder already exists with that name'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists', prepend=provider.prefix)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_with_folder_precheck_is_false(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists', prepend=provider.prefix)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path, folder_precheck=False)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/alreadyexists/')
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        create_url = generate_url_helper(key=path.full_path, method='PUT', expires=100, headers={}, query_parameters={})

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=403)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/alreadyexists/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }

        aiohttpretty.register_uri('GET', url, params=params, status=403)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/doesntalreadyexists/', prepend=provider.prefix)
        query_params = {
            'Prefix': path.full_path.lstrip('/'),
            'Delimiter': '/',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        url = generate_url_helper(method='GET', expires=100, headers={}, query_parameters=query_params)
        params = {
            'list-type': '2',
            'prefix': path.full_path.lstrip('/'),
            'delimiter': '/',
            'max-keys': '1000',
            'encoding-type': 'url'
        }
        create_url = generate_url_helper(key=path.full_path, method='PUT', expires=100, headers={}, query_parameters={})

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/' + path.path


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
        prefix = path.full_path.lstrip('/')
        url = generate_url_helper(
            method='GET',
            expires=100,
            headers={},
            query_parameters={
                'versions': '',
                'Prefix': prefix,
                'Delimiter': '/'
            }
        )
        params = {
            'versions': '',
            'prefix': prefix,
            'delimiter': '/',
            'encoding-type': 'url'
        }
        aiohttpretty.register_uri('GET', url, params=params, status=200, body=version_metadata)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 3

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_equality(self, provider, mock_time):
        assert not provider.can_intra_copy(provider)
        assert not provider.can_intra_move(provider)

    def test_can_intra_move(self, provider):

        file_path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
        folder_path = WaterButlerPath('/folder/', folder=True, prepend=provider.prefix)

        assert not provider.can_intra_move(provider)
        assert not provider.can_intra_move(provider, file_path)
        assert not provider.can_intra_move(provider, folder_path)

    def test_can_intra_copy(self, provider):

        file_path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
        folder_path = WaterButlerPath('/folder/', folder=True, prepend=provider.prefix)

        assert not provider.can_intra_copy(provider)
        assert not provider.can_intra_copy(provider, file_path)
        assert not provider.can_intra_copy(provider, folder_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_version_metadata(self, provider, single_version_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/single-version.file', prepend=provider.prefix)
        prefix = path.full_path.lstrip('/')
        url = generate_url_helper(
            method='GET',
            expires=100,
            headers={},
            query_parameters={
                'versions': '',
                'Prefix': prefix,
                'Delimiter': '/'
            }
        )
        params = {
            'versions': '',
            'prefix': prefix,
            'delimiter': '/',
            'encoding-type': 'url'
        }

        aiohttpretty.register_uri('GET',
                                  url,
                                  params=params,
                                  status=200,
                                  body=single_version_metadata)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 1

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
