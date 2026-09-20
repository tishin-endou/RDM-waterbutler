import os
import io
import xml
import json
import time
import base64
import hashlib
import asyncio
import logging
import datetime
import importlib

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
    """Build the error a storage rejection produces on the upload path.

    In production these errors are born in ``make_request`` (via
    ``exception_from_response``) and are tagged by ``_make_upload_request`` so
    that ``_translate_upload_error`` knows the payload is a raw storage
    response rather than a message WaterButler wrote itself.

    Tests that fake a storage failure above the ``make_request`` boundary have
    to reproduce that tag, and must do it by calling the provider's own
    ``_mark_storage_response`` -- re-implementing the tag here would let the
    test keep passing if the marker were renamed or its semantics changed.
    An *untagged* error carrying a storage body cannot occur in production, so
    asserting translation behaviour against one would test a state the
    provider never actually sees.
    """
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

# --- Material for the commit-notice cartesian product ----------------------
#
# Invariant: for the same operation and the same *observed* code, every
# transport reaches the same verdict.  Eight representative codes: 3
# definitive rejections (no notice), 3 indeterminate, 1 unknown, 1 missing.
#
# An unknown code and a missing code both fall to UNKNOWN.  That is the
# fail-safe direction: an over-reported notice costs the user a re-check,
# an under-reported one silently claims nothing was stored.
COMMIT_CODE_CASES = [
    ('AccessDenied', False),
    ('InvalidPart', False),
    ('EntityTooSmall', False),
    ('InternalError', True),
    ('SlowDown', True),
    ('RequestTimeout', True),
    ('XVendorMystery', True),
    (None, True),
]

# The classification table, written out independently of the implementation's
# ``DEFINITIVE_REJECTION_CODES``.  Generating it from the implementation would
# let a deleted row delete its own parameter, leaving that row unguarded.
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

# Transports where the code is observable.  3 x 8 = 24 cells.
OBSERVED_TRANSPORTS = ['direct_4xx', 'direct_5xx', 'complete_200_error']
# Transports where it is not.  2 x 8 = 16 cells: whatever the storage meant
# to say never reaches WaterButler, so the verdict is UNKNOWN regardless.
LATENT_TRANSPORTS = ['disconnect', 'broken_xml']


def commit_error_xml(error_code):
    """An S3 error body for CompleteMultipartUpload.

    An ``error_code`` of ``None`` yields a body with no ``<Code>`` element:
    the "missing code" cell, parsable but carrying no verdict.
    """
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
        # S3 reports a failed CompleteMultipartUpload as HTTP 200 + <Error>.
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=commit_error_xml(error_code).encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)
    elif transport == 'disconnect':
        # No response arrived, so no code is observable.  The S3 error XML is
        # put in the exception's ``message`` on purpose: ``_raw_error_body``
        # falls back to ``message``, so an implementation that reads a code
        # from there rather than from a response body has to fail here.
        provider.make_request = MockCoroutine(
            side_effect=aiohttp.ServerDisconnectedError(commit_error_xml(error_code)))
    elif transport == 'broken_xml':
        # The body arrived but is truncated.  The code string is present in it
        # yet cannot be parsed, so it is not observed -- a substring match must
        # never pick it up.
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
    """An ``aiohttp.web`` server that accepts a single commit.

    ``aiohttpretty`` injects responses *above* ``ClientSession._request``, so
    the redirect following that happens *inside* that call cannot be
    reproduced with it, and pinning it needs a real socket.  The real socket
    also pins, against a real ``ClientResponse``, the premise the three
    mock-injection cells lean on -- that the injection point is correct.

    Startup and teardown are owned here.  With ``runner.setup()`` through URL
    assembly left outside the ``finally``, a failure after the server started
    would carry a listening socket and the provider's sessions into the next
    test.

    ``runner.setup()`` itself is not guarded.  In aiohttp 3.6.2
    ``BaseRunner.cleanup()`` returns immediately while ``self._server is
    None``, so calling it after a failed setup does nothing, and the
    ``Application`` used here registers no ``on_startup``/``on_cleanup`` --
    there is no path by which a partial setup leaves resources behind.
    """

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
            # aiohttp 3.6.2 exposes the bound port only here.  If this private
            # attribute disappears the AttributeError is deliberate: a test
            # that visibly breaks beats one that quietly skips.
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
            # One failing close must not strand the rest: letting the loop
            # raise would leave every later session open and carry it into
            # the next test.
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
    
    # Mock datetime for boto3/botocore signature generation
    # 1454684930.0 corresponds to 2016-02-05 15:08:50 UTC
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
    """Helper to generate presigned URLs for boto3-based S3CompatSigV4Provider
    
    """
    def _generate_url(key=None, method='GET', expires=100, query_parameters=None, 
                     response_headers=None, headers=None, encrypt_key=False):
        """
        Generate a presigned URL for S3CompatSigV4Provider
        
        :param key: S3 object key (None for bucket-level operations like list_objects)
        :param method: HTTP method ('GET', 'HEAD', 'PUT', 'POST', 'DELETE')
        :param expires: Expiration time in seconds
        :param query_parameters: Additional query parameters dict (e.g., {'versions': '', 'delete': ''})
        :param response_headers: Response headers dict for presigned URLs
        :param headers: Request headers dict
        :param encrypt_key: Whether to use encryption (adds SSE headers)
        """
        method_upper = method.upper()
        
        # Map HTTP method to boto3 client method
        if key:
            # Object-level operations
            if method_upper == 'POST':
                if query_parameters and any(k.lower() == 'delete' for k in query_parameters.keys()):
                    client_method = 'delete_objects'
                elif query_parameters and 'uploads' in query_parameters:
                    client_method = 'create_multipart_upload'
                elif query_parameters and 'uploadId' in query_parameters:
                    client_method = 'complete_multipart_upload'
                else:
                    # Default POST operation (shouldn't happen in practice)
                    client_method = 'put_object'
            elif method_upper == 'DELETE':
                # Check if this is an abort multipart upload
                if query_parameters and 'uploadId' in query_parameters:
                    client_method = 'abort_multipart_upload'
                else:
                    client_method = 'delete_object'
            elif method_upper == 'GET':
                # Check if this is a list parts operation
                if query_parameters and 'uploadId' in query_parameters:
                    client_method = 'list_parts'
                else:
                    client_method = 'get_object'
            elif method_upper == 'PUT':
                # Check if this is an upload part operation
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
            # Bucket-level operations (list, bulk delete, etc.)
            if query_parameters and 'versions' in query_parameters:
                client_method = 'list_object_versions'
            elif query_parameters and any(k.lower() == 'delete' for k in query_parameters.keys()):
                client_method = 'delete_objects'
            else:
                client_method = 'list_objects_v2'
            params = {'Bucket': provider.bucket_name}
        
        # Add query parameters to params
        if query_parameters:
            # Handle special query parameters
            for key_param, value_param in query_parameters.items():
                # Skip query params that are only used to determine the boto3 method
                if key_param.lower() in ['versions', 'delete', 'uploads']:
                    continue
                # Convert S3 query parameter names to boto3 parameter names
                if key_param == 'uploadId':
                    params['UploadId'] = value_param
                elif key_param == 'partNumber':
                    params['PartNumber'] = int(value_param)
                elif key_param in ['prefix', 'delimiter']:
                    params[key_param.capitalize()] = value_param
                elif key_param in ['Prefix', 'Delimiter', 'VersionIdMarker', 'KeyMarker', 'VersionId']:
                    # Already in boto3 format
                    params[key_param] = value_param
                else:
                    params[key_param] = value_param
        
        # Add response headers (for download URLs)
        if response_headers:
            for rh_key, rh_value in response_headers.items():
                # Convert to boto3 format (e.g., 'response-content-disposition' -> 'ResponseContentDisposition')
                param_key = ''.join(word.capitalize() for word in rh_key.replace('response-', '').split('-'))
                param_key = 'Response' + param_key
                params[param_key] = rh_value
        
        # Add encryption headers if needed
        if encrypt_key or headers:
            # Note: Encryption and custom headers in presigned URLs work differently in boto3
            # They need to be included when making the request, not in the presigned URL itself
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

        # aiohttpretty.register_uri uses shallow copy for headers.
        # Therefore, we need to use a deep copied dictionary for GET.
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
        # Set trigger for encrypt_key=True in s3compatsigv4.provider.upload
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

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_chunked(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._chunked_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._chunked_upload.assert_called_with(file_stream, path)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete(self, provider, upload_parts_headers_list, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

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
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        # NOTE: the failure must be injected into ``_upload_parts``, not
        # ``_upload_part``.  ``_chunked_upload`` only ever calls the former, so
        # a ``side_effect`` on the latter never fires.  (An earlier revision did
        # exactly that and the test passed only because the unmocked
        # ``_complete_multipart_upload`` hit aiohttpretty's "No URLs matching
        # POST ..." error -- i.e. it asserted nothing about the abort path.)
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.side_effect = Exception('error')
        provider._complete_multipart_upload = MockCoroutine()
        provider._abort_chunked_upload = MockCoroutine()
        provider._abort_chunked_upload.return_value = True

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)
        # The abort has SUCCEEDED (return value True), so the "manual clean-up"
        # warning must NOT be appended to the error message.
        msg = 'An unexpected error has occurred during the multi-part upload.'
        assert str(exc.value) == ', '.join(['500', msg])

        provider._create_upload_session.assert_called_with(path)
        provider._upload_parts.assert_called_with(file_stream, path, upload_id)
        provider._abort_chunked_upload.assert_called_with(path, upload_id)
        # The parts upload failed, so the commit step must never be attempted.
        provider._complete_multipart_upload.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_unexpected_error_is_a_500(self, provider, file_stream,
                                                            mock_time):
        # Every other raise on this path is a 502, so the bare ``UploadError``
        # at the end looks like a missed ``code=``.  It is not: an exception
        # that is neither an ``UploadError`` (the storage answered) nor a
        # connection error (the link failed) did not come from upstream, and a
        # 502 would blame the storage for a defect on this side.  Pin it so the
        # difference stays a decision rather than an oversight.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(side_effect=ValueError('a bug on this side'))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert exc.value.code != HTTPStatus.BAD_GATEWAY
        # Not a storage failure, so none of the upstream-facing wording applies.
        assert provider.CONNECTION_INTERRUPTED_MESSAGE not in exc.value.message
        assert provider.QUOTA_EXCEEDED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('fails_at, expect_notice', [
        ('parts', False),
        ('commit-request', True),
        ('commit-read', True),
    ])
    async def test_chunked_upload_500_branch_notices_only_a_commit_failure(
            self, provider, file_stream, mock_time, fails_at, expect_notice):
        # The third exit of ``_chunked_upload``: an exception that is neither
        # ``UploadError`` nor a connection error lands in the 500 arm.  It is
        # reachable -- ``asyncio.CancelledError`` derives from ``Exception``
        # but from neither ``aiohttp.ClientError`` nor ``asyncio.TimeoutError``.
        #
        # Injection stays at the *boundaries* of ``_complete_multipart_upload``
        # (the commit request and the commit answer) and never replaces the
        # method with a mock raising an already-marked exception.  The code
        # under test has to be the thing that applies the mark, or narrowing
        # its ``except Exception``, or moving the mark inside an ``isinstance``
        # guard, leaves this test green.
        assert issubclass(asyncio.CancelledError, Exception)
        assert not issubclass(asyncio.CancelledError, pd_provider.CONNECTION_ERRORS)

        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

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
            # Cancelled during the parts: no commit was sent, so no assembly
            # can have started and the notice must stay off.
            provider._upload_parts = MockCoroutine(
                side_effect=asyncio.CancelledError('cancelled during the parts'))
            provider._make_upload_request = MockCoroutine()
        else:
            provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"e"'}])
            if fails_at == 'commit-request':
                # Cancelled while sending: no answer came back, so whether
                # the storage began assembling is unknowable.
                provider._make_upload_request = MockCoroutine(
                    side_effect=asyncio.CancelledError('cancelled while sending the commit'))
            else:
                # The answer came back but could not be read, and the body
                # is the only place the commit's outcome is written.
                provider._make_upload_request = MockCoroutine(
                    return_value=_AnswerWeCannotRead())

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert (provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message) is expect_notice
        if fails_at == 'parts':
            # Not one byte of the commit went out.
            provider._make_upload_request.assert_not_called()
        else:
            assert provider._make_upload_request.call_count == 1
        # Where an answer arrived, the connection is released even unread.
        assert released == ([True] if fails_at == 'commit-read' else [])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_abort_failure_appends_warning(self, provider, file_stream,
                                                                mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.side_effect = Exception('error')
        provider._abort_chunked_upload = MockCoroutine()
        provider._abort_chunked_upload.return_value = False

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)
        # The abort has FAILED (return value False), so the "manual clean-up"
        # warning must be appended to the error message.
        msg = 'An unexpected error has occurred during the multi-part upload.'
        msg += '  The abort action failed to clean up the temporary file parts generated ' \
               'during the upload process.  Please manually remove them.'
        assert str(exc.value) == ', '.join(['500', msg])

        provider._abort_chunked_upload.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_storage_quota_exceeded(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.side_effect = storage_error({'response': error_xml}, code=403)
        provider._abort_chunked_upload = MockCoroutine()
        provider._abort_chunked_upload.return_value = True

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        # A storage-side quota error must surface as HTTP 507 with an explicit,
        # user-readable message, and the multipart session must be aborted.
        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert 'QuotaExceeded' in exc.value.message
        provider._abort_chunked_upload.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_quota_exceeded_and_abort_fails(self, provider, file_stream,
                                                                 mock_time):
        # Worst case: the quota error and the abort failure have
        # to be reported together.  The abort warning is threaded through
        # _translate_upload_error as ``extra_message``, so it is easy to drop
        # while keeping both single-fault tests green.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(
            side_effect=storage_error({'response': error_xml}, code=403))
        provider._abort_chunked_upload = MockCoroutine(return_value=False)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert 'QuotaExceeded' in exc.value.message
        # The abort FAILED, so the manual clean-up warning must also be present.
        assert 'Please manually remove them.' in exc.value.message
        provider._abort_chunked_upload.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_contiguous_upload_storage_quota_exceeded(self, provider, file_stream,
                                                            mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        # ``make_request`` raises ``UploadError`` built by
        # ``exception_from_response`` when the storage rejects the PUT.
        provider.make_request = MockCoroutine()
        provider.make_request.side_effect = exceptions.UploadError({'response': error_xml},
                                                                   code=403)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._contiguous_upload(file_stream, path)

        # A storage-side quota error must surface as HTTP 507 with an explicit,
        # user-readable message.
        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert 'QuotaExceeded' in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_contiguous_upload_other_storage_error(self, provider, file_stream,
                                                         mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>AccessDenied</Code>'
                     '<Message>Access Denied</Message></Error>')

        provider.make_request = MockCoroutine()
        provider.make_request.side_effect = exceptions.UploadError({'response': error_xml},
                                                                   code=403)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._contiguous_upload(file_stream, path)

        # Non-quota errors keep the storage's status code but get a readable
        # message (not the raw XML body).
        assert exc.value.code == 403
        assert 'AccessDenied' in exc.value.message
        assert '<Error' not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_contiguous_upload_connection_interrupted(self, provider, file_stream,
                                                            mock_time):
        import aiohttp

        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        # Some storages close the connection mid-upload when the quota has been
        # exceeded; the raw client error must not propagate as an HTTP 500.
        provider.make_request = MockCoroutine()
        provider.make_request.side_effect = aiohttp.ClientOSError('Connection reset by peer')

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._contiguous_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert provider.CONNECTION_INTERRUPTED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_contiguous_upload_timeout(self, provider, file_stream, mock_time):
        # ``asyncio.TimeoutError`` is NOT an ``aiohttp.ClientError``, so the
        # whole-request timeout (AIOHTTP_TIMEOUT) used to escape untranslated
        # and reach the user as an unexplained HTTP 500.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        provider.make_request = MockCoroutine()
        provider.make_request.side_effect = asyncio.TimeoutError()

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._contiguous_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert provider.CONNECTION_INTERRUPTED_MESSAGE in exc.value.message
        # Only quota exhaustion is the user's to resolve.  A
        # dropped connection is an infrastructure fault and must keep paging.
        assert exc.value.is_user_error is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_exception_from_response_contract_xml(self, provider, mock_time,
                                                        generate_url_helper):
        # The whole quota translation depends on exception_from_response putting
        # an XML body into ``data['response']``.  Every other quota test builds
        # that shape by hand, so this one pins down the actual contract: if
        # ``exception_from_response`` ever changes (e.g. resp.json() starts
        # succeeding), the translation breaks silently and only this test fails.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        url = generate_url_helper(key=path.full_path, method='PUT', expires=100,
                                  headers={}, query_parameters={})
        error_body = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<Error><Code>QuotaExceeded</Code>'
                      '<Message>The bucket quota has been exceeded</Message></Error>')
        aiohttpretty.register_uri('PUT', url, status=403, body=error_body,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.UploadError) as exc:
            await provider.make_request('PUT', url, expects=(HTTPStatus.OK, ),
                                        throws=exceptions.UploadError)

        assert exc.value.data == {'response': error_body}
        assert provider._parse_s3_error_body(exc.value) == (
            'QuotaExceeded', 'The bucket quota has been exceeded')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_exception_from_response_contract_head(self, provider, mock_time,
                                                         generate_url_helper):
        # HEAD responses have no body, so exception_from_response produces a
        # plain string message.  _parse_s3_error_body must degrade to
        # (None, None) rather than raising on that shape.
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
        # End-to-end over a simulated HTTP exchange: no hand-built UploadError,
        # so make_request / exception_from_response / _parse_s3_error_body /
        # _translate_upload_error are all exercised together.
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
    async def test_chunked_upload_create_session_timeout(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        provider._create_upload_session = MockCoroutine(side_effect=asyncio.TimeoutError())

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert provider.CONNECTION_INTERRUPTED_MESSAGE in exc.value.message
        assert exc.value.is_user_error is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_parts_timeout(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'

        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(side_effect=asyncio.TimeoutError())
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        # A timeout is a connection-level failure, not an "unexpected error".
        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert provider.CONNECTION_INTERRUPTED_MESSAGE in exc.value.message
        assert exc.value.is_user_error is False
        provider._abort_chunked_upload.assert_called_with(path, upload_id)
        # The parts never finished uploading, so no commit was ever sent.  This
        # path *does* consult ``_commit_outcome_note``, so the absence has to be
        # asserted here rather than assumed.
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_entry_log_omits_raw_body(self, provider, file_stream,
                                                           mock_time, caplog):
        # ``'{!r}'.format(UploadError(...))`` renders the *whole* message, and
        # for a dict message that message is the storage's raw body serialised
        # as JSON -- unbounded, and duplicated a few lines later by
        # ``_translate_upload_error``.  The entry log must only say what kind of
        # failure it was; the body belongs to the single bounded log in the
        # translator.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        error_xml = ('<Error><Code>AccessDenied</Code>'
                     '<RequestId>TESTREQUESTID</RequestId>'
                     '<Message>{}</Message></Error>').format('y' * 4096)

        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(side_effect=storage_error(
            {'response': error_xml}, code=HTTPStatus.FORBIDDEN))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.UploadError):
                await provider._chunked_upload(file_stream, path)

        records = [r.getMessage() for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 2

        entry, translated = records
        # The entry log identifies the failure by type and status only.
        assert 'UploadError' in entry
        assert str(int(HTTPStatus.FORBIDDEN)) in entry
        assert upload_id in entry
        assert 'TESTREQUESTID' not in entry
        assert 'y' * 64 not in entry
        # The body survives exactly once, bounded by ERROR_BODY_LOG_LIMIT.
        assert 'TESTREQUESTID' in translated
        assert 'y' * pd_provider.ERROR_BODY_LOG_LIMIT not in translated
        # Neither record may be unbounded.
        for record in records:
            assert len(record) < 1024

    def test_connection_interrupted_message_does_not_assert_capacity(self, provider):
        # A dropped connection is only *evidence* of a full
        # storage -- it is equally often a network fault.  The message must not
        # send the user off to free up space when nothing is full.
        message = provider.CONNECTION_INTERRUPTED_MESSAGE
        assert 'network' in message.lower()
        assert 'may indicate' in message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('error_code,status,expected_level', [
        # Quota exhaustion is expected and the user can fix it themselves --
        # ``_translate_upload_error`` already logs it at WARNING and marks it
        # ``is_user_error``.  The entry log has to agree, or this line alone
        # keeps paging oncall every time somebody fills a bucket.
        ('QuotaExceeded', HTTPStatus.FORBIDDEN, logging.WARNING),
        ('XMinioStorageFull', HTTPStatus.FORBIDDEN, logging.WARNING),
        # ...including the 507 fallback, where the code is unrecognised.
        ('SomeVendorCode', HTTPStatus.INSUFFICIENT_STORAGE, logging.WARNING),
        # A real fault must still be an error: the downgrade must not be blanket.
        ('AccessDenied', HTTPStatus.FORBIDDEN, logging.ERROR),
        ('InternalError', HTTPStatus.INTERNAL_SERVER_ERROR, logging.ERROR),
    ])
    async def test_chunked_upload_entry_log_level_follows_quota(
            self, provider, file_stream, mock_time, caplog, error_code, status, expected_level):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>{}</Code><Message>nope</Message></Error>').format(error_code)

        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(
            side_effect=storage_error({'response': error_xml}, code=status))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.UploadError):
                await provider._chunked_upload(file_stream, path)

        entry = [r for r in caplog.records
                 if r.name == PROVIDER_LOGGER and 'multi-part upload' in r.getMessage()]
        assert len(entry) == 1
        assert entry[0].levelno == expected_level

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_entry_log_requires_the_storage_tag(
            self, provider, file_stream, mock_time, caplog):
        # Every other case above builds its error with ``storage_error``, which
        # tags it.  So deleting the ``_is_storage_response`` guard from
        # ``_is_quota_exhaustion`` leaves the whole suite green while the
        # predicate silently starts trusting messages WaterButler wrote itself.
        # A 507 that carries no tag must stay an ERROR.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        # Deliberately untagged: this is the shape of an error WaterButler
        # authored, not one built from a storage response.
        provider._upload_parts = MockCoroutine(side_effect=exceptions.UploadError(
            'WaterButler wrote this', code=HTTPStatus.INSUFFICIENT_STORAGE))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.UploadError):
                await provider._chunked_upload(file_stream, path)

        entry = [r for r in caplog.records
                 if r.name == PROVIDER_LOGGER and 'multi-part upload' in r.getMessage()]
        assert len(entry) == 1
        assert entry[0].levelno == logging.ERROR

    def test_passthrough_preserves_is_user_error(self, provider):
        # The untagged branch rebuilds the exception to append the abort
        # warning.  Dropping ``is_user_error`` there promotes a failure the user
        # caused from Sentry's info level to error (server/api/v1/core.py).
        err = exceptions.UploadError('WaterButler wrote this', code=HTTPStatus.CONFLICT,
                                     is_user_error=True)

        translated = provider._translate_upload_error(err, extra_message='  Abort failed.')

        assert translated.is_user_error is True
        assert translated.code == HTTPStatus.CONFLICT
        assert 'Abort failed.' in translated.message

    def test_passthrough_is_observable(self, provider, caplog):
        # An untagged error reaching the translator is accepted as normal, so a
        # new upload call site that forgets ``_make_upload_request`` degrades
        # silently: quota errors stop becoming 507s and nothing fails.  A debug
        # line is the only thing that makes the omission findable in the field.
        err = exceptions.UploadError('WaterButler wrote this', code=HTTPStatus.CONFLICT)

        with caplog.at_level(logging.DEBUG, logger=PROVIDER_LOGGER):
            provider._translate_upload_error(err)

        assert [r for r in caplog.records
                if r.name == PROVIDER_LOGGER and r.levelno == logging.DEBUG
                and 'untagged' in r.getMessage()]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('stage', ['contiguous', 'create-session', 'chunked'])
    async def test_translated_error_does_not_chain_the_raw_body(self, provider, file_stream,
                                                                mock_time, stage):
        # ``raise translated`` inside an ``except`` block sets ``__context__``
        # to the untranslated error, so the raw body comes back in the rendered
        # traceback -- which is what the logs and Sentry show.  Stripping the
        # body from the message accomplishes nothing if the chained exception
        # carries it anyway.
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
        # The translated error only carries a summary message, so the raw body
        # (RequestId / Resource) is the only way to investigate afterwards.  It
        # used to be dropped entirely on the contiguous path.
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
        # A non-quota storage rejection is a genuine error.
        assert records[0].levelno == logging.ERROR

    def test_translate_upload_error_log_truncates_body(self, provider, caplog):
        # An unbounded body would flood the log; storages can return very large
        # error documents (or a proxy's HTML error page).
        #
        # The bound is declared here as a literal rather than read from the
        # module: deriving it from the constant makes the test agree with
        # whatever the constant happens to say, so shrinking it to 10 (or
        # growing it to 1 MB) would keep this green.  512 is the reviewed
        # value, so changing it has to be a deliberate edit here too.
        limit = 512
        assert pd_provider.ERROR_BODY_LOG_LIMIT == limit
        error_xml = '<Error><Code>AccessDenied</Code><Message>{}</Message></Error>'.format(
            'x' * (limit * 8))
        err = storage_error({'response': error_xml}, code=HTTPStatus.FORBIDDEN)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            provider._translate_upload_error(err)

        logged = [r for r in caplog.records if r.name == PROVIDER_LOGGER][0].getMessage()
        # Pin the bound itself, not just "shorter than the input": the body is
        # cut at exactly ERROR_BODY_LOG_LIMIT bytes and no further.
        assert error_xml[:limit] in logged
        assert error_xml[:limit + 1] not in logged
        # Nothing else in the record may reintroduce the rest of the body.
        assert len(logged) < limit * 2

    def test_translate_upload_error_log_bound_is_in_bytes(self, provider, caplog):
        # The constant is declared as "how much of the raw error body is written
        # to the log", and the scenario its comment names -- a misconfigured
        # proxy answering with an HTML page -- is measured in bytes.  A
        # character-based cut lets a multibyte body through at three times the
        # declared size, which is exactly the case a non-English deployment
        # hits first.
        limit = 512
        assert pd_provider.ERROR_BODY_LOG_LIMIT == limit
        error_xml = '<Error><Code>AccessDenied</Code><Message>{}</Message></Error>'.format(
            '\u3042' * limit)
        err = storage_error({'response': error_xml}, code=HTTPStatus.FORBIDDEN)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            provider._translate_upload_error(err)

        logged = [r for r in caplog.records if r.name == PROVIDER_LOGGER][0].getMessage()
        # The body contribution is bounded in bytes, so it cannot exceed the
        # limit however wide the characters are.  (The prefix the log line adds
        # is ASCII and well under 512 bytes.)
        assert len(logged.encode('utf-8')) < limit * 2

    @pytest.mark.parametrize('body', [
        # Decodes to 1536 bytes, three times the declared limit: ``replace``
        # emits one U+FFFD per undecodable byte and U+FFFD is 3 bytes in
        # UTF-8.  Truncating bytes and then decoding is not enough.
        b'\xff' * 512,
        b'\xff' * 4096,
        # Valid multi-byte text: a cut inside a character adds 1-2 bytes.
        '\u3042' * 512,
        ('\u3042' * 512).encode('utf-8'),
        # Invalid bytes mixed with valid text, as a real proxy reply looks.
        b'<html>' + b'\xc3\x28' * 300 + '\u3042'.encode('utf-8') * 100,
        # Under the limit the body passes through: the over-trimming control.
        b'<Error><Code>AccessDenied</Code></Error>',
        '<Error><Code>AccessDenied</Code></Error>',
        b'',
        '',
    ])
    def test_bounded_body_never_exceeds_the_declared_limit(self, body):
        # The constant declares "the first ERROR_BODY_LOG_LIMIT *bytes* of the
        # raw error body", so the only way to check the declaration is to weigh
        # the return value in UTF-8 again.  Bounding the whole log line instead
        # misses inputs that inflate threefold on decode.
        bounded = pd_provider._bounded_body(body)

        assert len(bounded.encode('utf-8')) <= pd_provider.ERROR_BODY_LOG_LIMIT
        # Over-trimming check, decided on the *decoded* size.  Measuring the
        # input instead would make ``b'\xff' * 512`` (512 in, 1536 out)
        # contradict the assertion above and the test unsatisfiable.
        source = body if isinstance(body, bytes) else body.encode('utf-8')
        decoded = source.decode('utf-8', 'replace')
        if len(decoded.encode('utf-8')) <= pd_provider.ERROR_BODY_LOG_LIMIT:
            assert bounded == decoded
        else:
            # Size alone would also accept an implementation returning ``''``.
            # Staying under the limit and keeping the body are two separate
            # requirements and ``_bounded_body`` exists for the second one.
            #
            # "It is the leading part" is pinned as a prefix, not as a length:
            # where the cut lands depends on character width and on where the
            # invalid bytes sit.
            assert bounded
            assert decoded.startswith(bounded)
            # Most of the budget has to be used, which kills an implementation
            # returning a single character: even 3-byte units fill a third.
            assert len(bounded.encode('utf-8')) > pd_provider.ERROR_BODY_LOG_LIMIT // 3

    @pytest.mark.parametrize('body, expected', [
        # Invalid bytes: ``replace`` emits one U+FFFD per byte and U+FFFD is
        # 3 bytes in UTF-8, so 512 // 3 = 170 characters fit.
        (b'\xff' * 512, '\ufffd' * 170),
        # However long the input, the same amount survives.
        (b'\xff' * 4096, '\ufffd' * 170),
        # Valid 3-byte characters; the 2 leftover bytes go in the ``ignore`` pass.
        ('\u3042' * 512, '\u3042' * 170),
        (('\u3042' * 512).encode('utf-8'), '\u3042' * 170),
    ])
    def test_bounded_body_keeps_exactly_the_leading_bytes(self, body, expected):
        # "Is a prefix" plus "larger than a third of the limit" bottoms out at
        # 170 bytes, which an implementation trimming multi-byte bodies to 256
        # -- about half of what is kept today -- would still satisfy.
        #
        # Representative inputs therefore carry a complete expected value.  The
        # 170 is computed by hand from the declaration (U+FFFD is 3 bytes, the
        # limit is 512), not derived from the implementation.
        assert pd_provider.ERROR_BODY_LOG_LIMIT == 512
        assert pd_provider._bounded_body(body) == expected

    def test_bounded_body_passes_none_through(self):
        # An error built without a body yields ``None``.  Collapsing that to
        # ``''`` would make "the body was empty" and "there was no body"
        # indistinguishable in the log.
        assert pd_provider._bounded_body(None) is None

    def test_translate_upload_error_quota_logged_as_warning(self, provider, caplog):
        # Quota exhaustion is an expected, user-resolvable failure (see the
        # is_user_error handling), so it must not be logged at ERROR level and
        # trip the on-call alerting.
        error_xml = ('<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message>'
                     '<RequestId>QUOTAREQUESTID</RequestId></Error>')
        err = storage_error({'response': error_xml}, code=HTTPStatus.FORBIDDEN)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            provider._translate_upload_error(err)

        records = [r for r in caplog.records if r.name == PROVIDER_LOGGER]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert 'QUOTAREQUESTID' in records[0].getMessage()

    def test_quota_exceeded_error_codes_env_override(self):
        # ``SettingsDict.get`` always returns a ``str`` when the envvar is set,
        # which turns the ``error_code in ...`` membership test into substring
        # matching.  List settings must be read with ``get_object``.
        env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES':
               '["XMinioStorageFull"]'}
        try:
            with mock.patch.dict(os.environ, env):
                codes = importlib.reload(pd_settings).QUOTA_EXCEEDED_ERROR_CODES
        finally:
            # Restore the module *after* the envvar patch is undone, otherwise
            # the override leaks into every later test.
            importlib.reload(pd_settings)

        assert not isinstance(codes, str)
        assert 'XMinioStorageFull' in codes
        # A substring of a configured code must never be treated as a match.
        assert 'StorageFull' not in codes

    @pytest.mark.parametrize('label,raw,expected', [
        # ``get_object`` is ``json.loads`` with no type check, so the envvar can
        # legitimately decode to any JSON type.  Every one of them has to end up
        # as a set of strings, because the only consumer is ``code in codes``.
        ('json-array', '["XMinioStorageFull"]', {'XMinioStorageFull'}),
        # A quoted JSON scalar decodes to ``str``.  Feeding that to ``frozenset``
        # explodes it into one entry per character, so the configured code stops
        # matching entirely -- and nothing fails loudly.
        ('json-scalar-string', '"QuotaExceeded"', {'QuotaExceeded'}),
        # A JSON number is not iterable at all: ``frozenset(507)`` raises
        # ``TypeError`` while the settings module is being imported, which takes
        # the whole provider down rather than just mis-classifying an error.
        ('json-number', '507', {'507'}),
    ])
    def test_quota_exceeded_error_codes_env_types(self, label, raw, expected):
        # This must exercise the *real* path: envvar -> ``get_object`` ->
        # whatever normalisation the settings module does.  Patching the
        # already-computed attribute would skip exactly the code under test.
        env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES': raw}
        try:
            with mock.patch.dict(os.environ, env):
                codes = importlib.reload(pd_settings).QUOTA_EXCEEDED_ERROR_CODES
        finally:
            importlib.reload(pd_settings)

        assert set(codes) == expected
        # A substring of a configured code must never be treated as a match.
        for code in expected:
            assert code[:-1] not in codes

    @pytest.mark.parametrize('label,raw', [
        # The value operators are most likely to write: the bare error code,
        # without the JSON quoting ``get_object`` requires.
        ('bare-word', 'QuotaExceeded'),
        ('comma-separated', 'QuotaExceeded,XMinioStorageFull'),
        ('empty-string', ''),
        ('truncated-json', '["QuotaExceeded"'),
    ])
    def test_malformed_json_falls_back_instead_of_killing_the_import(self, label, raw):
        # ``_normalise_error_codes`` is applied to the *return value* of
        # ``get_object``, so it never sees a value that ``json.loads`` refused.
        # An import-time ``JSONDecodeError`` is not a loud failure: stevedore
        # turns the entry-point load error into ``ProviderNotFound``, so every
        # s3compatsigv4 request answers 404 while the process stays up and the
        # other providers keep working.  A quota-code typo must not do that.
        env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES': raw}
        try:
            with mock.patch.dict(os.environ, env):
                codes = importlib.reload(pd_settings).QUOTA_EXCEEDED_ERROR_CODES
        finally:
            importlib.reload(pd_settings)

        # Falling back to the defaults keeps quota detection working rather
        # than leaving it configured with a half-parsed value.
        assert 'QuotaExceeded' in codes
        assert 'XMinioStorageFull' in codes

    def test_malformed_json_warns(self, caplog):
        # The fallback is silent from the operator's point of view: quota
        # detection keeps working with the *defaults*, so the codes they
        # configured simply never match.  The warning is the only thing that
        # connects that symptom to its cause, and asserting the fallback value
        # alone does not notice it being demoted to DEBUG.
        with caplog.at_level(logging.WARNING, logger=pd_settings.__name__):
            env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES': 'QuotaExceeded'}
            with mock.patch.dict(os.environ, env):
                pd_settings._read_error_codes()

        records = [r for r in caplog.records if r.name == pd_settings.__name__]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert 'QUOTA_EXCEEDED_ERROR_CODES' in records[0].getMessage()
        assert 'not valid JSON' in records[0].getMessage()

    def test_mapping_config_warns(self, caplog):
        # Same reasoning as above, for the branch that silently reduced a
        # mapping to its keys before R4-B.
        with caplog.at_level(logging.WARNING, logger=pd_settings.__name__):
            pd_settings._normalise_error_codes({'QuotaExceeded': 507})

        records = [r for r in caplog.records if r.name == pd_settings.__name__]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert 'mapping' in records[0].getMessage()

    def test_null_config_falls_back_to_the_defaults(self):
        # ``null`` is valid JSON, so it reaches ``_normalise_error_codes``
        # intact.  ``None`` is not a ``Mapping``, not a ``str`` and not
        # ``Iterable``, so the scalar branch wrapped it and produced
        # ``frozenset({'None'})`` -- a configuration under which no storage
        # error code can ever match.  "Unset" is the only sane reading.
        assert pd_settings._normalise_error_codes(None) == frozenset(
            pd_settings.QUOTA_EXCEEDED_ERROR_CODE_DEFAULTS)

    def test_mapping_config_is_rejected_rather_than_silently_degraded(self):
        # A ``dict`` satisfies ``Iterable``, so it slips past the scalar branch
        # and ``frozenset(str(code) for code in ...)`` quietly reduces it to its
        # *keys*.  That is indistinguishable from a working configuration until
        # a quota error fails to be recognised in production.
        env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES': '{"QuotaExceeded": 507}'}
        try:
            with mock.patch.dict(os.environ, env):
                codes = importlib.reload(pd_settings).QUOTA_EXCEEDED_ERROR_CODES
        finally:
            importlib.reload(pd_settings)

        assert 'XMinioStorageFull' in codes

    def test_quota_error_codes_env_types_reach_the_provider(self, provider):
        # The normalisation is only useful if the value the provider actually
        # reads is the normalised one.
        env = {'S3COMPAT_PROVIDER_CONFIG_QUOTA_EXCEEDED_ERROR_CODES': '"QuotaExceeded"'}
        try:
            with mock.patch.dict(os.environ, env):
                importlib.reload(pd_settings)
                translated = provider._translate_upload_error(storage_error(
                    {'response': '<Error><Code>QuotaExceeded</Code></Error>'}, code=403))
        finally:
            importlib.reload(pd_settings)

        assert translated.code == HTTPStatus.INSUFFICIENT_STORAGE

    def test_quota_exceeded_error_codes_defaults(self):
        codes = pd_settings.QUOTA_EXCEEDED_ERROR_CODES
        # MinIO returns XMinioStorageFull on the S3 data path when the disk is
        # full; XMinioAdminBucketQuotaExceeded is the bucket-quota code.
        assert 'QuotaExceeded' in codes
        assert 'XMinioAdminBucketQuotaExceeded' in codes
        assert 'XMinioStorageFull' in codes
        # Not an S3 error code -- it is an HTTP reason phrase.
        assert 'InsufficientStorage' not in codes

    @pytest.mark.parametrize('label,configured,expected', [
        ('list', ['QuotaExceeded'], {'QuotaExceeded'}),
        # A bare ``str`` must be wrapped, not iterated: iterating it yields one
        # entry per character and ``'Quota' in 'QuotaExceeded'`` would have
        # turned the membership test into substring matching.
        ('bare-string', 'QuotaExceeded', {'QuotaExceeded'}),
        # A JSON number is not iterable, so it has to be wrapped before the
        # ``frozenset`` call rather than after it.
        ('bare-int', 507, {'507'}),
        ('mixed-list', ['QuotaExceeded', 507], {'QuotaExceeded', '507'}),
        ('tuple', ('QuotaExceeded',), {'QuotaExceeded'}),
    ])
    def test_normalise_error_codes(self, label, configured, expected):
        codes = pd_settings._normalise_error_codes(configured)

        assert isinstance(codes, frozenset)
        assert codes == expected
        # Every element is a ``str``, so ``code in codes`` can never raise.
        assert all(isinstance(code, str) for code in codes)

    @pytest.mark.parametrize('configured,warns', [
        (['QuotaExceeded'], False),
        ('QuotaExceeded', True),
        (507, True),
    ])
    def test_normalise_error_codes_warns_on_scalar(self, caplog, configured, warns):
        # A scalar is coerced, not rejected: raising here would happen at import
        # time and take the provider down over a typo.  The warning is the only
        # signal the operator gets, so it must actually be emitted.
        with caplog.at_level(logging.WARNING, logger=pd_settings.__name__):
            pd_settings._normalise_error_codes(configured)

        records = [r for r in caplog.records if r.name == pd_settings.__name__]
        assert bool(records) is warns
        if warns:
            assert 'QUOTA_EXCEEDED_ERROR_CODES' in records[0].getMessage()

    def test_user_facing_messages_keep_their_wording(self, provider):
        # Every other assertion in this file spells the expected text as
        # ``provider.<CONSTANT>``, so changing a constant changes the assertion
        # with it.  Setting one to ``''`` makes ``'' in message`` vacuously true
        # and deletes the assertion outright -- measured: emptying
        # QUOTA_EXCEEDED_MESSAGE killed zero tests.
        #
        # Pinning the wording once, here, is what gives those assertions teeth.
        # It is deliberately partial (phrases, not the full string) so that
        # rewording for clarity stays cheap while deletion and replacement do
        # not.  This is the same guard H-7 added for
        # UPLOAD_MAY_HAVE_COMPLETED_MESSAGE, which had not been carried across
        # to the other three constants.
        assert 'quota or capacity' in provider.QUOTA_EXCEEDED_MESSAGE
        assert 'free up storage space' in provider.QUOTA_EXCEEDED_MESSAGE

        assert 'could not be ' in provider.UNCLASSIFIED_STORAGE_ERROR_MESSAGE
        assert 'interpreted' in provider.UNCLASSIFIED_STORAGE_ERROR_MESSAGE
        assert 'retry the upload' in provider.UNCLASSIFIED_STORAGE_ERROR_MESSAGE

        assert 'connection to the cloud storage was interrupted' \
            in provider.CONNECTION_INTERRUPTED_MESSAGE
        # The message must keep naming the network as a possible cause: a
        # dropped connection is evidence of exhausted capacity, never proof.
        assert 'network problem' in provider.CONNECTION_INTERRUPTED_MESSAGE

        assert 'may in fact have completed' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        assert 'check the file list' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    def test_translate_upload_error_507_fallback(self, provider):
        # The storage may answer 507 with an error code we do not know.  The
        # status alone is enough to treat it as a quota failure.
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>SomeVendorSpecificCode</Code>'
                     '<Message>no space left</Message></Error>')
        err = storage_error({'response': error_xml}, code=HTTPStatus.INSUFFICIENT_STORAGE)

        translated = provider._translate_upload_error(err)
        assert translated.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in translated.message
        assert 'SomeVendorSpecificCode' in translated.message
        # The 507 fallback is a quota failure like any other, so
        # it must get the same non-paging treatment as a recognised code.
        assert translated.is_user_error is True

    def test_translate_upload_error_507_without_xml_body(self, provider):
        # A 507 with an unparsable body must still become a quota message.
        err = storage_error('Insufficient Storage', code=HTTPStatus.INSUFFICIENT_STORAGE)

        translated = provider._translate_upload_error(err)
        assert translated.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in translated.message
        assert translated.is_user_error is True

    def test_translate_upload_error_quota_is_user_error(self, provider):
        # Filling up a bucket is an expected user-side failure: it must not be
        # reported to Sentry at error level nor page oncall via 5xx alerts.
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')
        err = storage_error({'response': error_xml}, code=403)

        assert provider._translate_upload_error(err).is_user_error is True

        # Non-quota storage errors are not the user's doing.
        other_xml = error_xml.replace('QuotaExceeded', 'AccessDenied')
        other = storage_error({'response': other_xml}, code=403)
        assert provider._translate_upload_error(other).is_user_error is False

    @pytest.mark.parametrize('label,err_factory', [
        # Quota, by error code.
        ('quota-code', lambda: storage_error(
            {'response': '<Error><Code>QuotaExceeded</Code>'
                         '<Message>quota</Message>'
                         '<Resource>/bucket/secret-key-name</Resource></Error>'}, code=403)),
        # Quota, by HTTP 507 fallback.
        ('quota-507', lambda: storage_error(
            {'response': '<Error><Code>Whatever</Code>'
                         '<Resource>/bucket/secret-key-name</Resource></Error>'},
            code=HTTPStatus.INSUFFICIENT_STORAGE)),
        # Classified, but not quota.
        ('other-code', lambda: storage_error(
            {'response': '<Error><Code>AccessDenied</Code><Message>nope</Message>'
                         '<Resource>/bucket/secret-key-name</Resource></Error>'}, code=403)),
        # Unclassifiable XML.
        ('unclassifiable', lambda: storage_error(
            {'response': '<Error><Resource>/bucket/secret-key-name</Resource></Error>'},
            code=HTTPStatus.BAD_GATEWAY)),
        # Not XML at all.
        ('non-xml', lambda: storage_error(
            {'response': '/bucket/secret-key-name is over quota'}, code=403)),
        # ``exception_from_response`` builds this shape for a HEAD/no-body
        # response: a *string* message that embeds the presigned URL.
        ('default-msg', lambda: storage_error(
            'An error occurred while making a PUT request to '
            'https://host/bucket/secret-key-name?X-Amz-Signature=deadbeef', code=403)),
    ])
    @pytest.mark.parametrize('extra_message', ['', '  abort failed'])
    def test_translate_upload_error_never_leaks_raw_body(self, provider, label, err_factory,
                                                         extra_message):
        # ``BaseHandler.write_error`` (server/api/v1/core.py:28) writes
        # ``exc.data`` verbatim as the response body when it is truthy, and
        # otherwise writes ``exc.message``.  Either way, anything left on the
        # translated exception is shown to the user -- including the storage's
        # Resource paths and, on the string-message path, the presigned URL and
        # its signature.  The translated error must carry a summary only.
        err = err_factory()
        translated = provider._translate_upload_error(err, extra_message=extra_message)

        assert translated.data is None, 'raw body would be written as the response body'
        assert 'secret-key-name' not in translated.message
        assert 'X-Amz-Signature' not in translated.message
        if extra_message:
            assert translated.message.endswith(extra_message)

    def test_check_for_200_error_preserves_error_body(self, provider):
        # S3 signals CompleteMultipartUpload failures with HTTP 200 plus an
        # <Error> body.  The raw body must survive on the exception, otherwise
        # the quota translation downstream has nothing to work with.
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

    @pytest.mark.parametrize('label,error_xml', [
        # ``xmltodict`` collapses all three of these to ``{'Error': None}``, so a
        # lookup that returns ``None`` cannot tell "no <Error> element" from
        # "<Error> element we failed to classify".  Conflating the two makes a
        # failed CompleteMultipartUpload look like a success.
        ('empty-element', '<?xml version="1.0" encoding="UTF-8"?><Error/>'),
        ('empty-pair', '<?xml version="1.0" encoding="UTF-8"?><Error></Error>'),
        ('whitespace-only', '<?xml version="1.0" encoding="UTF-8"?><Error>   </Error>'),
    ])
    def test_check_for_200_error_fails_closed_on_empty_error_element(self, provider, label,
                                                                    error_xml):
        # An <Error> element is present: the request failed.  Not being able to
        # classify it is no reason to report success.
        with pytest.raises(exceptions.UploadError):
            provider._check_for_200_error(error_xml.encode('utf-8'),
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)

    @pytest.mark.parametrize('label,error_xml', [
        ('missing-code',
         '<?xml version="1.0" encoding="UTF-8"?>'
         '<Error><Message>something went wrong</Message></Error>'),
        ('empty-code',
         '<?xml version="1.0" encoding="UTF-8"?>'
         '<Error><Code/><Message>something went wrong</Message></Error>'),
    ])
    def test_check_for_200_error_unclassifiable_is_not_a_server_fault(self, provider, label,
                                                                     error_xml):
        # The user must never see a bare HTTP 500.  An <Error>
        # body we cannot classify is the *storage* answering unintelligibly, so
        # it is a bad-gateway condition, not a WaterButler bug.
        with pytest.raises(exceptions.UploadError) as exc:
            provider._check_for_200_error(error_xml.encode('utf-8'),
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)

        assert int(exc.value.code) != int(HTTPStatus.INTERNAL_SERVER_ERROR)
        assert int(exc.value.code) == int(HTTPStatus.BAD_GATEWAY)

    def test_check_for_200_error_malformed_xml_is_controlled(self, provider):
        # A truncated body raises ExpatError out of ``xmltodict``.  Letting it
        # escape means an HTTP 500 with a stack trace (``_translate_upload_error``
        # then trips over the missing ``.message``).
        with pytest.raises(exceptions.UploadError) as exc:
            provider._check_for_200_error(b'<Error><Code>QuotaExceeded',
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)

        assert int(exc.value.code) == int(HTTPStatus.BAD_GATEWAY)

    def test_check_for_200_error_accepts_success_body(self, provider):
        # Guard the other direction: a genuine success body must stay silent.
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<CompleteMultipartUploadResult><ETag>"etag"</ETag>'
                '</CompleteMultipartUploadResult>')
        provider._check_for_200_error(body.encode('utf-8'), 'CompleteMultipartUpload',
                                      exceptions.UploadError)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('error_body', [
        b'<?xml version="1.0" encoding="UTF-8"?><Error/>',
        b'<?xml version="1.0" encoding="UTF-8"?><Error>   </Error>',
        b'<?xml version="1.0" encoding="UTF-8"?><Error><Message>nope</Message></Error>',
        b'<Error><Code>QuotaExceeded',
    ])
    async def test_chunked_upload_complete_200_with_unclassifiable_error(
            self, provider, file_stream, mock_time, error_body):
        # The regression this pins down: on a *replace* upload the old object is
        # still in the bucket, so ``upload()`` would return its metadata and the
        # caller would record a successful upload of data that was never
        # committed.  The session must be aborted and the caller must see an
        # error.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

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

        assert int(exc.value.code) != int(HTTPStatus.INTERNAL_SERVER_ERROR)
        provider._abort_chunked_upload.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_session_error_keeps_waterbutler_message(
            self, provider, file_stream, mock_time):
        # ``_create_upload_session`` authors its own 502: a session may exist on
        # the storage but its UploadId is unknown, so it cannot be aborted and
        # an administrator has to remove it by hand.  That message has no
        # storage body behind it, so ``_translate_upload_error`` must not
        # replace it with the generic "could not be interpreted" text.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

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
    @pytest.mark.parametrize('stage', ['create-session', 'upload-part', 'complete'])
    async def test_every_upload_stage_tags_its_storage_errors(self, provider, file_stream,
                                                              mock_time, stage):
        # ``_translate_upload_error`` only translates errors the upload path
        # tagged in ``_make_upload_request``.  A call site that reaches for
        # ``make_request`` directly therefore stops being translated *silently*:
        # the user gets the storage's raw 403 instead of the quota message.
        #
        # Every other test for these three stages mocks above ``make_request``
        # (``_create_upload_session`` / ``_upload_parts`` are replaced wholesale),
        # so none of them would notice the tag going missing.  This one mocks the
        # boundary itself, which keeps each real call site under test.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')
        # Deliberately *untagged*: ``make_request`` is the boundary where the tag
        # is applied, so tagging it here would defeat the purpose of the test.
        failure = exceptions.UploadError({'response': error_xml}, code=403)

        provider._abort_chunked_upload = MockCoroutine(return_value=True)
        if stage != 'create-session':
            provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        if stage == 'complete':
            provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])

        provider.make_request = MockCoroutine(side_effect=failure)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('error_body', [
        b'<Error/>',
        b'<Error><Message>no code here</Message></Error>',
        b'<Error><Code>QuotaExceeded',
    ])
    async def test_chunked_upload_complete_unclassifiable_warns_upload_may_exist(
            self, provider, file_stream, mock_time, error_body):
        # Fail-closed is kept, but the complete may in fact
        # have succeeded -- we simply could not read the answer.  Telling the
        # user only that the upload failed invites a duplicate.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

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
        # Asserting against the constant alone passes for *any* value of it,
        # including ``''``.  Pinning the wording here is what makes the
        # assertion above detect the message being emptied (the same failure
        # mode the ERROR_BODY_LOG_LIMIT test was fixed for).
        assert 'may in fact have completed' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        assert 'check the file list' in provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        # The raw body must still not reach the user.
        assert 'Error' not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_read_failure_warns_upload_may_exist(
            self, provider, file_stream, mock_time):
        # The commit was sent and the storage answered -- we just could not read
        # the answer.  This is the case the notice exists for, yet it
        # was the one case that did not get it: ``_mark_commit_outcome_unknown``
        # sat behind ``except exceptions.UploadError``, which a dropped
        # connection does not satisfy.  The user was told the upload was
        # "interrupted before the upload completed" and asked to retry.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        resp = mock.Mock()
        resp.read = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message
        # The connection message must not contradict the notice it now carries.
        assert 'before the upload completed' not in exc.value.message
        # The connection was still released despite the read blowing up.
        assert resp.release.called

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_parts_connection_error_does_not_claim_a_commit(
            self, provider, file_stream, mock_time):
        # This test used to break the connection at *session creation*, which
        # raises out of ``_chunked_upload``'s first ``except`` -- a branch that
        # never calls ``_commit_outcome_note`` at all.  Asserting the notice's
        # absence there asserted nothing: the mutation that makes the note
        # unconditional left this test green.
        #
        # The branch that does consult the note is the connection-error arm of
        # the outer handler, so break the connection during ``_upload_parts``
        # instead.  The parts never finished, so no commit was ever sent and
        # the notice must stay off.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.CONNECTION_INTERRUPTED_MESSAGE in exc.value.message
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_parts_connection_error_can_claim_a_commit(
            self, provider, file_stream, mock_time):
        # Positive control for the test above.  Without it, "the notice is
        # absent" is indistinguishable from "this branch can never emit the
        # notice" -- which is precisely the defect being fixed.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        dropped = pd_provider._mark_commit_outcome_unknown(aiohttp.ServerDisconnectedError())
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(side_effect=dropped)
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_200_with_error_quota(self, provider, file_stream,
                                                                mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        provider._create_upload_session = MockCoroutine(return_value=upload_id)
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        # CompleteMultipartUpload answers 200 with an error body.
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=error_xml.encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        # The user must not see a bare HTTP 500.
        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert 'QuotaExceeded' in exc.value.message
        provider._abort_chunked_upload.assert_called_with(path, upload_id)
        # The storage answered the commit and named the reason, so the outcome
        # is *not* unknown.  Saying "your quota is exhausted" and "the upload
        # may in fact have completed" in the same breath is self-contradictory,
        # and it is the combination this exact response produces in production.
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('error_code', [
        # In the classification table, so the notice is suppressed.
        'AccessDenied', 'EntityTooLarge',
        # Not in the table, but the quota branch runs first and suppresses it.
        'QuotaExceeded',
    ])
    async def test_complete_200_with_error_code_suppresses_the_notice(
            self, provider, file_stream, mock_time, error_code):
        # The claim is that the *code* suppresses the notice -- a table hit or
        # the quota branch -- not that the 200 transport does.  Asserting on
        # the marker ``_check_for_200_error`` sets could not tell the two
        # apart.  Transport independence is covered by the cartesian product.
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>{}</Code><Message>boom</Message></Error>'.format(error_code))
        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=error_xml.encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('body', [
        # Unparsable: the storage said *something* went wrong but not what.
        b'<<< not xml at all',
        # An ``<Error>`` with no usable ``Code`` is equally uninformative.
        b'<?xml version="1.0" encoding="UTF-8"?><Error><Message>boom</Message></Error>',
        b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>   </Code></Error>',
    ])
    async def test_complete_200_without_an_error_code_still_warns(
            self, provider, file_stream, mock_time, body):
        # Suppression is allowed only where the storage actually reached a
        # verdict.  With no readable code the commit's outcome is genuinely
        # unknown, so the notice stays (``None`` is UNKNOWN).
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._create_upload_session = MockCoroutine(return_value='EXAMPLEUPLOADID')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': '"etag1"'}])
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body)
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport', OBSERVED_TRANSPORTS)
    @pytest.mark.parametrize('error_code,expect_notice', COMMIT_CODE_CASES)
    async def test_commit_notice_depends_only_on_the_observed_code(
            self, provider, file_stream, mock_time, transport, error_code, expect_notice):
        # The 24 observable cells: the same operation and the same observed
        # code must give the same verdict on every transport.  Per-transport
        # parameter sets cannot expose a contradiction between transports,
        # which is why the product is taken in one place.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        arrange_commit_failure(provider, transport, error_code)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert (provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message) is expect_notice

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport', LATENT_TRANSPORTS)
    @pytest.mark.parametrize('latent_code', [code for code, _ in COMMIT_CODE_CASES])
    async def test_commit_notice_when_the_code_cannot_be_observed(
            self, provider, file_stream, mock_time, transport, latent_code, monkeypatch):
        # The remaining 16 cells.  On a disconnect or a broken body
        # ``_parse_s3_error_body`` returns no code, so whatever the storage
        # meant to say, the verdict has to fall to UNKNOWN.
        #
        # These cells are not vacuous: the code string really is there -- in
        # the exception's ``message`` on a disconnect, inside the truncated
        # body on broken XML.  An implementation reading it from anywhere but
        # a parsed body, or by substring, drops the notice and fails here.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        arrange_commit_failure(provider, transport, latent_code)

        # "Not observable" is the premise of these cells, so the premise is
        # asserted alongside the conclusion: an implementation emitting the
        # notice unconditionally would satisfy the conclusion on its own.
        observed = []
        real_observed_error_code = pd_provider.S3CompatSigV4Provider._observed_error_code

        def spy(err):
            code = real_observed_error_code(err)
            observed.append(code)
            return code

        monkeypatch.setattr(pd_provider.S3CompatSigV4Provider,
                            '_observed_error_code', staticmethod(spy))

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message
        assert observed and all(code is None for code in observed)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport', OBSERVED_TRANSPORTS)
    @pytest.mark.parametrize('error_code', ['QuotaExceeded',
                                            'XMinioAdminBucketQuotaExceeded',
                                            'XMinioStorageFull'])
    async def test_quota_branch_suppresses_the_notice_on_every_transport(
            self, provider, file_stream, mock_time, transport, error_code):
        # Quota codes are deliberately absent from the classification table:
        # MinIO does not enforce quota on the commit path, so they are no
        # guarantee that nothing was committed.  The quota branch runs ahead
        # of the table instead and suppresses the notice -- "you are out of
        # space" next to "it may have completed" is the contradiction this
        # change exists to remove.
        #
        # The 200 transport alone cannot pin this, because there the marker
        # has already emptied the notice; the 5xx transports are what make a
        # regression in the quota branch visible.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        arrange_commit_failure(provider, transport, error_code)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_quota_507_without_a_body_suppresses_the_notice(self, provider, file_stream,
                                                                  mock_time):
        # ``_is_quota_exhaustion`` treats HTTP 507 as quota exhaustion whatever
        # the body says -- a deliberate fallback, since vendor-specific codes
        # cannot be enumerated.  The table knows nothing about this input, so
        # moving the suppression into the table brings the "507 plus notice"
        # contradiction straight back.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider.make_request = MockCoroutine(
            side_effect=exceptions.UploadError('no body', code=507))

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport', OBSERVED_TRANSPORTS)
    @pytest.mark.parametrize('error_code, expect_notice', [
        # Surrounding whitespace is stripped before matching: pretty-printed
        # XML produces ``<Code>\n  AccessDenied\n</Code>``.
        ('\n  AccessDenied\n', False),
        (' AccessDenied ', False),
        ('\tEntityTooSmall  ', False),
        # Case is not folded: S3 error codes are identifiers that match down
        # to the case across vendors, so folding would create false matches.
        ('accessdenied', True),
        ('ACCESSDENIED', True),
        # No substring matching, in either direction.
        ('AccessDeniedByPolicy', True),
        ('XAccessDenied', True),
        # Inner whitespace is not part of the identifier and is not removed
        # either: the match is exact, so this falls to UNKNOWN, the safe side.
        ('Access Denied', True),
    ])
    async def test_commit_notice_follows_the_code_matching_rules(
            self, provider, file_stream, mock_time, transport, error_code, expect_notice):
        # The code-matching rules: case-sensitive, surrounding whitespace
        # stripped, no substring match.  They are design requirements, not
        # incidental behaviour, so they are pinned as a product even where the
        # implementation happens to get them for free -- the point is to pin
        # the dependency on that behaviour.
        assert file_stream.size == 6
        arrange_chunked_commit(provider)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        arrange_commit_failure(provider, transport, error_code)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert (provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message) is expect_notice

    def test_the_xml_parser_is_what_strips_the_code(self, provider):
        # xmltodict strips text nodes by default, which makes the ``.strip()``
        # in ``_parse_s3_error_body`` redundant today, and therefore invisible
        # to the matching-rule test above.  That is only acceptable while
        # "somebody strips" stays observable: if a dependency bump stops
        # xmltodict from stripping, this test fails and the ``.strip()`` is
        # the only thing still holding the rule up.
        parsed = xmltodict.parse('<Error><Code>\n  AccessDenied\n</Code></Error>')
        assert parsed['Error']['Code'] == 'AccessDenied'

    @pytest.mark.parametrize('raw, expected', [
        ('\n  QuotaExceeded\n', True),
        ('quotaexceeded', False),
        ('XQuotaExceededFoo', False),
    ])
    def test_quota_detection_follows_the_same_matching_rules(self, provider, raw, expected):
        # The same matching rules apply to the quota codes.  If the two lists
        # diverge, only one of them mishandles a whitespace-padded code.
        err = storage_error({'response': commit_error_xml(raw)}, code=400)
        assert provider._is_quota_exhaustion(err) is expected

    @pytest.mark.parametrize('code', [
        # No status at all: the connection dropped.
        #
        # 507 is deliberately absent: ``_is_quota_exhaustion`` counts it as
        # quota exhaustion whatever the body says, so it is the one status the
        # notice *does* depend on -- and it is a suppression, not a class rule.
        # ``test_quota_507_without_a_body_suppresses_the_notice`` covers it.
        None, 500, 503, 403, 400, 499,
        # A redirect is a misconfiguration (e.g. the wrong region), not a
        # failed commit.
        302,
        # ``code`` is not guaranteed to be an int: ``exception_from_response``
        # passes through whatever the caller supplied.
        '502', 'boom',
    ])
    @pytest.mark.parametrize('error_code,not_committed', [
        ('AccessDenied', True),
        ('InternalError', False),
        (None, False),
    ])
    def test_commit_outcome_note_ignores_the_status_class(self, provider, code, error_code,
                                                          not_committed):
        # ``_check_for_200_error`` synthesises HTTP 502 for *every*
        # 200-with-``<Error>`` body -- the shape a failed
        # CompleteMultipartUpload actually takes -- so the status reads "5xx"
        # for responses the storage was perfectly definite about.  Any rule
        # that consults it is deciding on an artefact of WaterButler's own
        # error construction.
        err = pd_provider._mark_commit_outcome_unknown(pd_provider._mark_storage_response(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=code)))
        note = provider._commit_outcome_note(err)
        assert (note == '') is not_committed
        assert (note == provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE) is not not_committed

    def test_the_definitive_rejection_table_is_exactly_the_designed_nine(self):
        # Pin the table's contents as a whole.  The test below guards each
        # row; this one guards against rows being added, which suppresses a
        # notice that should have been shown -- the direction that needs an
        # administrator to recover.
        assert pd_provider.DEFINITIVE_REJECTION_CODES == frozenset(DEFINITIVE_REJECTION_CODES)

    @pytest.mark.parametrize('error_code', DEFINITIVE_REJECTION_CODES)
    def test_every_definitive_rejection_code_suppresses_the_notice(self, provider, error_code):
        # All nine rows, so that deleting a single code is caught.  The
        # cartesian product runs only three representatives; without this the
        # other six would be rows nobody checks.
        #
        # The parameters come from the copy at the top of this file, not from
        # ``pd_provider.DEFINITIVE_REJECTION_CODES``: parameters generated
        # from the implementation cannot test the implementation.
        err = pd_provider._mark_commit_outcome_unknown(pd_provider._mark_storage_response(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=400)))
        assert provider._commit_outcome_note(err) == ''

    @pytest.mark.parametrize('error_code', [
        # Deliberately left out of the table: ``NoSuchUpload`` also comes back
        # from a resend after a *successful* first commit, so it is no
        # evidence that nothing was committed.
        'NoSuchUpload',
        # Real 4xx codes absent from the table.  Over-reporting the notice is
        # the intended direction for anything the table does not name.
        'InvalidRequest', 'BadDigest', 'RequestTimeTooSkewed', 'NoSuchKey', 'TooManyParts',
        # Indeterminate and unknown codes.
        'InternalError', 'SlowDown', 'ServiceUnavailable', 'RequestTimeout', 'XVendorMystery',
        # Wrong case and substrings count as absent, per the matching rules.
        'accessdenied', 'ACCESSDENIED', 'XAccessDeniedFoo', 'AccessDeniedExtra',
        # No code could be read.
        None,
    ])
    def test_codes_outside_the_table_keep_the_notice(self, provider, error_code):
        # Everything outside the table keeps the notice, so that forgetting to
        # extend the table errs towards over-reporting rather than silence.
        err = pd_provider._mark_commit_outcome_unknown(pd_provider._mark_storage_response(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=400)))
        assert provider._commit_outcome_note(err) == provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    def test_commit_outcome_note_does_not_read_a_code_off_a_connection_error(self, provider):
        # ``_raw_error_body`` falls back to ``err.message`` when there is no
        # body, and aiohttp's connection errors carry a message of their own.
        # Without the gate, a response that never arrived would get a say in
        # the classification table.  A dropped connection observed no code.
        err = pd_provider._mark_commit_outcome_unknown(
            aiohttp.ServerDisconnectedError(commit_error_xml('AccessDenied')))
        assert pd_provider._is_storage_response(err) is False
        assert provider._commit_outcome_note(err) == provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    def test_commit_outcome_note_needs_the_mark(self, provider):
        # Without the mark there was never a commit in flight, whatever the
        # status says.
        err = exceptions.UploadError('boom', code=500)
        assert provider._commit_outcome_note(err) == ''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('chunked', [False, True])
    async def test_connection_error_log_does_not_leak_the_signature(
            self, provider, file_stream, mock_time, caplog, chunked):
        # aiohttp 3.6.2 builds this exact message in ``ClientRequest.write_bytes``
        # when the socket dies mid-body:
        #
        #     new_exc = ClientOSError(exc.errno,
        #                             'Can not write request body for %s' % self.url)
        #
        # and ``self.url`` is the presigned URL this provider signs with SigV4.
        # A dropped connection during the upload is precisely the event this PR
        # exists to handle, so this is the common path, not a corner case:
        # logging the exception renders the signature into the log and, through
        # the exception chain, into the traceback Sentry keeps.
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
        # The type is what the log is for, so it still has to be there.
        assert 'ClientOSError' in logged
        # The chained ``__context__`` renders the original exception -- and its
        # message -- into the traceback, so suppressing the log alone is not
        # enough.  The translation paths already use ``from None``; the
        # connection paths have to match.
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
        # ``_chunked_upload`` has seven ``raise ... from None`` sites.  The two
        # taken when the *commit* fails -- the ``CONNECTION_ERRORS`` arm and
        # the 500 arm -- are guarded nowhere else:
        # ``test_connection_error_log_does_not_leak_the_signature`` kills
        # ``make_request`` outright and so never gets past
        # ``_create_upload_session``.  This test reaches the commit.
        #
        # What leaks is the signature query of the presigned SigV4 URL,
        # carried into the traceback through ``__context__`` and kept by
        # Sentry.  Suppressing it is observable: ``__suppress_context__``.
        presigned = ('https://minio.example/bkt/key?X-Amz-Algorithm=AWS4-HMAC-SHA256'
                     '&X-Amz-Credential=AKIAEXAMPLE%2F20260913%2Fus-east-1%2Fs3%2Faws4_request'
                     '&X-Amz-Signature=1f2e3d4c5b6a7988SECRETSIG')
        if failure == 'connection':
            err = aiohttp.ClientOSError(
                32, 'Can not write request body for {}'.format(presigned))
        else:
            # Neither ``UploadError`` nor ``CONNECTION_ERRORS``: the 500 arm.
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
        # Confirm the arm under test is the one that actually ran.
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_session_parse_failure_log_omits_repr(self, provider, mock_time, caplog):
        # Same rule for the CreateMultipartUpload parse failure: identify the
        # error by type, and bound the body with the shared constant rather
        # than a second hard-coded 512.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        body = b'<InitiateMultipartUploadResult>' + b'z' * 4096

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body)
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with caplog.at_level(logging.DEBUG, logger=PROVIDER_LOGGER):
            with pytest.raises(exceptions.UploadError):
                await provider._create_upload_session(path)

        logged = '\n'.join(r.getMessage() for r in caplog.records if r.name == PROVIDER_LOGGER)
        assert 'ExpatError' in logged
        # ``{!r}`` on the exception is what pulls arbitrary upstream text into
        # the log; the type name carries the diagnostic value here.
        assert 'ExpatError(' not in logged
        assert len(logged) < 2 * pd_provider.ERROR_BODY_LOG_LIMIT

    def test_parse_s3_error_body_non_xml(self, provider):
        err = storage_error({'response': 'not xml at all'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)
        # Unclassifiable, so the translator cannot say anything specific -- but
        # it must not hand the raw body back to the caller either.
        translated = provider._translate_upload_error(err)
        assert translated is not err
        assert translated.data is None
        assert 'not xml at all' not in translated.message

    def test_parse_s3_error_body_pretty_printed(self, provider):
        # Storages are free to pretty-print their XML.  Surrounding whitespace
        # must not defeat the quota lookup.
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                     '<Error>\n'
                     '  <Code>\n    QuotaExceeded\n  </Code>\n'
                     '  <Message>\n    The bucket quota has been exceeded\n  </Message>\n'
                     '</Error>\n')
        err = storage_error({'response': error_xml}, code=403)

        assert provider._parse_s3_error_body(err)[0] == 'QuotaExceeded'
        translated = provider._translate_upload_error(err)
        assert translated.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in translated.message

    def test_parse_s3_error_body_scalar_error_element(self, provider):
        # ``xmltodict`` maps an element without children to a plain string, so
        # ``parsed['Error']`` is not always a dict.
        err = storage_error({'response': '<Error>something went wrong</Error>'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)
        translated = provider._translate_upload_error(err)
        assert translated is not err
        assert translated.data is None

    def test_parse_s3_error_body_empty_code_element(self, provider):
        # An empty ``<Code/>`` becomes ``None``; an element with attributes only
        # becomes a dict.  Neither is a usable error code.
        err = exceptions.UploadError(
            {'response': '<Error><Code/><Message>nope</Message></Error>'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)

        err = exceptions.UploadError(
            {'response': '<Error><Code lang="en"/></Error>'}, code=500)
        assert provider._parse_s3_error_body(err) == (None, None)

    def test_parse_s3_error_body_repeated_code_elements(self, provider):
        # ``xmltodict`` collapses repeated siblings into a list, so a malformed
        # or merged error document gives ``Code`` as ``['A', 'QuotaExceeded']``.
        # Picking one of them would be guesswork, so this is unclassifiable --
        # but it must stay safe end to end: no crash from ``.strip()`` on a
        # list, no HTTP 500, and no raw body handed to the user.
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>SlowDown</Code><Code>QuotaExceeded</Code>'
                     '<Message>first</Message><Message>second</Message>'
                     '<Resource>/bucket/secret-key-name</Resource></Error>')
        err = storage_error({'response': error_xml}, code=403)

        assert provider._parse_s3_error_body(err) == (None, None)

        translated = provider._translate_upload_error(err)
        assert int(translated.code) == 403
        assert translated.data is None
        assert 'secret-key-name' not in translated.message

        # ...and the 200-with-error path still fails closed on it.
        with pytest.raises(exceptions.UploadError) as exc:
            provider._check_for_200_error(error_xml.encode('utf-8'),
                                          'CompleteMultipartUpload',
                                          exceptions.UploadError)
        assert int(exc.value.code) == int(HTTPStatus.BAD_GATEWAY)

    def test_parse_s3_error_body_namespaced(self, provider):
        # Some S3-compatible storages emit namespace-prefixed error documents.
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<s3:Error xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">'
                     '<s3:Code>QuotaExceeded</s3:Code>'
                     '<s3:Message>The bucket quota has been exceeded</s3:Message>'
                     '</s3:Error>')
        err = storage_error({'response': error_xml}, code=403)

        assert provider._parse_s3_error_body(err) == (
            'QuotaExceeded', 'The bucket quota has been exceeded')
        translated = provider._translate_upload_error(err)
        assert translated.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in translated.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_session_quota_exceeded(self, provider, file_stream,
                                                                mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>QuotaExceeded</Code>'
                     '<Message>The bucket quota has been exceeded</Message></Error>')

        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.side_effect = storage_error(
            {'response': error_xml}, code=403)
        provider._abort_chunked_upload = MockCoroutine()

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)

        assert exc.value.code == HTTPStatus.INSUFFICIENT_STORAGE
        assert provider.QUOTA_EXCEEDED_MESSAGE in exc.value.message
        # No session was created, so nothing must be aborted.
        provider._abort_chunked_upload.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_upload_session_invalid_response(self, provider, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=b'this is not the expected xml')
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._create_upload_session(path)

        # A malformed 200-range response must become a controlled error, not a
        # raw ExpatError/KeyError propagating as HTTP 500.
        assert exc.value.code == HTTPStatus.BAD_GATEWAY
        assert 'unexpected response' in exc.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('upload_id_xml', [
        '<UploadId/>',
        '<UploadId>   </UploadId>',
        '<UploadId attr="x"/>',
    ])
    async def test_create_upload_session_blank_upload_id(self, provider, mock_time,
                                                         upload_id_xml):
        # Well-formed XML with an unusable UploadId must not be returned: every
        # later request would be signed with ``None`` and fail obscurely.
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
    @pytest.mark.parametrize('body,raises', [
        ('<?xml version="1.0" encoding="UTF-8"?>'
         '<CompleteMultipartUploadResult><ETag>"etag"</ETag>'
         '</CompleteMultipartUploadResult>', False),
        ('<?xml version="1.0" encoding="UTF-8"?>'
         '<Error><Code>QuotaExceeded</Code></Error>', True),
        ('<Error><Code>QuotaExceeded', True),
    ])
    async def test_complete_multipart_upload_always_releases(self, provider, mock_time,
                                                             body, raises):
        # ``release()`` came after the error check, so the 200-with-error path
        # skipped it and leaked the connection back-pressure -- on exactly the
        # path a quota-exhausted storage takes for every single upload.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body.encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        if raises:
            with pytest.raises(exceptions.UploadError):
                await provider._complete_multipart_upload(path, 'EXAMPLEUPLOADID',
                                                          [{'ETAG': '"etag1"'}])
        else:
            await provider._complete_multipart_upload(path, 'EXAMPLEUPLOADID',
                                                      [{'ETAG': '"etag1"'}])

        assert resp.release.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_multipart_upload_releases_when_read_fails(self, provider, mock_time):
        # ``read()`` sat *outside* the try, so a connection dropped mid-body --
        # the common failure once the storage is struggling -- skipped the
        # ``finally`` entirely and leaked the connection.  The read is part of
        # what has to be cleaned up after, so it belongs inside the try.
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
    async def test_complete_multipart_upload_request_failure_is_marked(self, provider, mock_time):
        # Three ways a commit can end with its outcome unknown; this is the
        # first -- the commit request itself dies mid-flight.  The ``except``
        # around it is deliberately ``Exception`` rather than ``UploadError``,
        # because a dropped connection is not an ``UploadError``, and that is
        # precisely the case where the storage may have received the whole
        # request and assembled the object anyway.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider.make_request = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())

        with pytest.raises(aiohttp.ServerDisconnectedError) as exc:
            await provider._complete_multipart_upload(path, 'EXAMPLEUPLOADID',
                                                      [{'ETAG': '"etag1"'}])

        assert pd_provider._is_commit_outcome_unknown(exc.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_upload_session_releases_when_read_fails(self, provider, mock_time):
        # Same defect ``_complete_multipart_upload`` had, 300 lines earlier: a
        # storage running out of room drops the connection while the body is
        # being read, and without a ``finally`` the connection leaks -- on the
        # path that is by definition already under pressure.
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
    async def test_create_upload_session_releases_on_success(self, provider, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        body = ('<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult>'
                '<UploadId>EXAMPLEUPLOADID</UploadId>'
                '</InitiateMultipartUploadResult>').encode('utf-8')

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body)
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        assert await provider._create_upload_session(path) == 'EXAMPLEUPLOADID'
        assert resp.release.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_releases_when_read_fails(self, provider, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)

        resp = mock.Mock()
        resp.status = HTTPStatus.OK
        resp.read = MockCoroutine(side_effect=aiohttp.ServerDisconnectedError())
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(aiohttp.ServerDisconnectedError):
            await provider._list_uploaded_chunks(path, 'EXAMPLEUPLOADID')

        assert resp.release.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_upload_session_strips_upload_id(self, provider, mock_time):
        # The response is parsed with ``strip_whitespace=False`` (needed
        # elsewhere), so a pretty-printed UploadId keeps its surrounding
        # newlines and indentation.  Returning it unstripped puts whitespace
        # into every following request's ``uploadId`` query parameter -- and
        # into the SigV4 signature -- so the parts would be signed for an
        # upload id the storage does not have.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        body = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<InitiateMultipartUploadResult>\n'
                '  <UploadId>\n    EXAMPLEUPLOADID\n  </UploadId>\n'
                '</InitiateMultipartUploadResult>\n')

        resp = mock.Mock()
        resp.read = MockCoroutine(return_value=body.encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)

        assert await provider._create_upload_session(path) == 'EXAMPLEUPLOADID'

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
        # aiohttp resp headers use upper case
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
        # aiohttp resp headers use upper case
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
        # aiohttp resp headers are upper case
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
        # aiohttp resp headers are upper case
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
        # aiohttp resp headers are upper case
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
        # The notice is decided on the code alone, and that code has to be the
        # result of the *only* attempt.  Under core's default ``retry=2`` a
        # resend after a 504 meets a consumed UploadId and gets
        # ``NoSuchUpload`` even though the first commit succeeded; once the
        # observed code is the last attempt's, the table is meaningless.
        #
        # Counting the POSTs over real HTTP pins that ``retry=0`` takes
        # effect.  Inspecting the caller only pins that it is written down.
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

        # Pin the retried statuses too, so that widening core's ``retry_on``
        # reports this parameter set as no longer covering it.
        assert provider._retry_on == {408, 502, 503, 504}
        assert status in provider._retry_on
        assert len(aiohttpretty.calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize('redirect_status', [307, 308])
    async def test_complete_multipart_upload_does_not_follow_a_redirect(
            self, provider, upload_parts_headers_list, mock_time, redirect_status):
        # ``retry=0`` stops only core's own retry loop in ``make_request``.  A
        # 307/308 says "resend with the method and body intact", and aiohttp
        # follows it itself under the default ``allow_redirects=True``, so two
        # commit POSTs go out without spending any of core's retry budget.
        # If the second is refused -- consumed UploadId, signature for another
        # host -- the observed code becomes the second attempt's, and a
        # definitive rejection there would claim nothing was stored even
        # though the first commit succeeded.
        #
        # ``aiohttpretty`` cannot pin this; see ``commit_server``.
        calls = []

        async def first(request):
            await request.read()
            calls.append(request.path)
            raise web.HTTPTemporaryRedirect(location='/second') \
                if redirect_status == 307 else web.HTTPPermanentRedirect(location='/second')

        async def second(request):
            # The second POST, reached only if the redirect were followed.  It
            # answers with a definitive rejection code -- the "no notice"
            # side -- so that following the redirect fails towards the
            # dangerous verdict rather than a harmless one.
            await request.read()
            calls.append(request.path)
            return web.Response(
                status=403, content_type='application/xml',
                text='<?xml version="1.0" encoding="UTF-8"?><Error>'
                     '<Code>SignatureDoesNotMatch</Code>'
                     '<Message>The request signature we calculated does not match.</Message>'
                     '</Error>')

        app = web.Application()
        app.router.add_post('/first', first)
        app.router.add_post('/second', second)

        async with commit_server(provider, app) as server:
            path = WaterButlerPath('/foobah', prepend=provider.prefix)
            headers_list = json.loads(upload_parts_headers_list).get('headers_list')
            headers_list = [{k.upper(): v for k, v in headers.items()}
                            for headers in headers_list]

            with mock.patch.object(provider.connection, 'generate_presigned_url',
                                   return_value=server.url):
                with pytest.raises(exceptions.UploadError):
                    await provider._complete_multipart_upload(
                        path, 'EXAMPLEUPLOADID', headers_list)

        # Exactly one commit POST.  A second one records ``/second``, so a
        # failure here shows how far the request got.
        assert calls == ['/first']

    @pytest.mark.asyncio
    async def test_commit_answer_that_cannot_be_decoded_still_notices(
            self, provider, file_stream, mock_time):
        # When the returned body is not valid UTF-8, core's
        # ``exception_from_response`` raises ``UnicodeDecodeError`` mid-decode.
        # That is neither ``UploadError`` nor a connection error, so it lands
        # in the 500 arm -- but the commit is already on the socket, so
        # whether assembly started is unknown.  Which layer produces the
        # exception may move with core; the notice must not.
        #
        # This runs over a real socket to pin, with a real ``ClientResponse``,
        # the injection-point premise the mock-injection cells rely on.
        # Redirects are incidental: a plain 403 takes the same path.
        calls = []

        async def first(request):
            await request.read()
            calls.append(request.path)
            # Declared as XML, but the bytes are not valid UTF-8.
            return web.Response(status=403, content_type='application/xml',
                                body=b'\xff\xfe<Error><Code>AccessDenied</Code></Error>')

        app = web.Application()
        app.router.add_post('/first', first)

        async with commit_server(provider, app) as server:
            arrange_chunked_commit(provider)
            with mock.patch.object(provider.connection, 'generate_presigned_url',
                                   return_value=server.url):
                with pytest.raises(exceptions.UploadError) as exc:
                    await provider._chunked_upload(
                        file_stream, WaterButlerPath('/foobah', prepend=provider.prefix))

        assert calls == ['/first']
        # Not provably a storage verdict, so 500.  The code was never read, so
        # the table does not apply and this falls to UNKNOWN with the notice.
        assert exc.value.code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in exc.value.message
        # The unreadable body must not reach the user verbatim.
        assert 'AccessDenied' not in exc.value.message

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
        # The session is gone, which is precisely the state
        # abort is trying to reach.  Retrying until the cap and then reporting
        # failure sends the user hunting for parts that do not exist.
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
        # Decision (B): ListParts is what the docstring names as the criterion
        # for a successful abort, so ``NoSuchUpload`` is confirmed rather than
        # trusted.  One extra request; still no retry budget burned.
        assert len(aiohttpretty.calls) == 2

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_no_such_upload_with_parts_left_still_warns(
            self, provider, mock_time, generate_url_helper):
        # ``NoSuchUpload`` has three causes: the commit succeeded, the session
        # was already aborted, or the session expired by TTL.  Only the third
        # can leave parts behind, and S3-compatible storages do not all match
        # AWS here.  Treating the code alone as proof would suppress the
        # "please remove them manually" warning exactly when it is needed.
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
        parts_left = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<ListPartsResult><Part><PartNumber>1</PartNumber>'
                      '<ETag>"etag1"</ETag></Part></ListPartsResult>')
        aiohttpretty.register_uri('GET', list_url, body=parts_left.encode('utf-8'), status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aborted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_is_fail_closed(self, provider, mock_time):
        # Decision (B) rests entirely on this: the confirmation exists because
        # ``NoSuchUpload`` on the DELETE does not establish that the *parts*
        # are gone, and resolving an unanswerable question in favour of
        # "already clean" would suppress the "remove them manually" warning in
        # exactly the case it is needed.  Nothing was holding that line.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._list_uploaded_chunks = MockCoroutine(side_effect=Exception('boom'))

        assert await provider._abort_confirmed_by_list_parts(path, 'EXAMPLEUPLOADID') is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_failure_still_warns(self, provider, mock_time,
                                                          generate_url_helper):
        # The same thing through the public entry point: an unconfirmable abort
        # has to report failure so the caller appends the manual-cleanup notice.
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
        provider._list_uploaded_chunks = MockCoroutine(side_effect=Exception('boom'))

        assert await provider._abort_chunked_upload(path, upload_id) is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_is_a_single_round_trip(self, provider, mock_time,
                                                             generate_url_helper):
        # Decision (A), 2026-09-13: the confirmation buys safety for the cost of
        # one round trip, and that is the whole bargain.  Nested inside the
        # abort retry loop *and* inside make_request's own retry budget it was
        # worth up to 2 x 3 = 6 ListParts requests against a storage that is
        # already struggling.  It runs once, on the first iteration.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100,
                                        headers={}, query_parameters=params)
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100,
                                       headers={}, query_parameters=params)
        no_such_upload = ('<?xml version="1.0" encoding="UTF-8"?>'
                          '<Error><Code>NoSuchUpload</Code>'
                          '<Message>The specified upload does not exist.</Message></Error>')
        parts_left = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<ListPartsResult><Part><PartNumber>1</PartNumber>'
                      '<ETag>"etag1"</ETag></Part></ListPartsResult>')
        aiohttpretty.register_uri('DELETE', abort_url, body=no_such_upload.encode('utf-8'),
                                  status=404)
        aiohttpretty.register_uri('GET', list_url, body=parts_left.encode('utf-8'), status=200)

        assert await provider._abort_chunked_upload(path, upload_id) is False
        assert pd_settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES == 2
        # Second iteration re-sends the DELETE but does not re-ask ListParts.
        assert [c['method'] for c in aiohttpretty.calls] == ['DELETE', 'GET', 'DELETE']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_confirmation_does_not_use_the_retry_budget(self, provider, mock_time):
        # The other half of "one round trip": make_request retries 408/502/503/504
        # twice by default, with a 2s then 4s sleep.  A confirmation that cannot
        # be obtained has to fall through to the caller's own retry, not stall
        # the upload response for six seconds inside a fail-closed check.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._list_uploaded_chunks = MockCoroutine(side_effect=Exception('boom'))

        await provider._abort_confirmed_by_list_parts(path, 'EXAMPLEUPLOADID')

        assert provider._list_uploaded_chunks.call_args[1]['retry'] == 0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('status', [408, 502, 503, 504])
    async def test_abort_confirmation_is_sent_exactly_once(
            self, provider, mock_time, generate_url_helper, status):
        # The test above shows only that ``_abort_confirmed_by_list_parts``
        # *passes* ``retry=0``.  If ``_list_uploaded_chunks`` stopped
        # forwarding ``**request_kwargs`` to ``make_request``, the argument
        # would be dropped and core's default ``retry=2`` would apply.
        # Counting GETs through the real core path pins that it takes effect.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100,
                                       headers={}, query_parameters=params)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>SlowDown</Code>'
                     '<Message>Please reduce your request rate.</Message></Error>')
        aiohttpretty.register_uri('GET', list_url, body=error_xml.encode('utf-8'), status=status)

        try:
            # No confirmation: the claim stays unestablished, so fall through
            # to the caller's own retry.
            assert await provider._abort_confirmed_by_list_parts(path, upload_id) is False
        finally:
            for session in provider.session_list:
                await session.close()

        assert status in provider._retry_on
        assert len(aiohttpretty.calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_parts_errors_carry_the_storage_tag(self, provider, mock_time,
                                                           generate_url_helper):
        # Same rule as the abort DELETE: a body may only be read as a storage
        # response when the tag says so.  ListParts is the request the abort
        # confirmation depends on, and an untagged error there degrades quietly
        # -- ``_translate_upload_error`` would stop turning it into a 507.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        list_url = generate_url_helper(key=path.full_path, method='GET', expires=100,
                                       headers={}, query_parameters=params)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>AccessDenied</Code><Message>nope</Message></Error>')
        aiohttpretty.register_uri('GET', list_url, body=error_xml.encode('utf-8'), status=403)

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._list_uploaded_chunks(path, upload_id)

        assert pd_provider._is_storage_response(exc.value)

    @pytest.mark.parametrize('body,expected', [
        ('<ListPartsResult></ListPartsResult>', True),
        ('<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>', True),
        # A truncated listing with no ``Part`` element in *this* page says
        # nothing about the pages after it.  Reading it as "zero parts" is a
        # false positive on the side that suppresses the manual-cleanup
        # warning, so the user never learns that billable parts remain.
        ('<ListPartsResult><IsTruncated>true</IsTruncated></ListPartsResult>', False),
        ('<ListPartsResult><IsTruncated>true</IsTruncated>'
         '<Part><PartNumber>1</PartNumber></Part></ListPartsResult>', False),
    ])
    def test_no_parts_left_respects_is_truncated(self, provider, body, expected):
        assert provider._no_parts_left(
            ('<?xml version="1.0" encoding="UTF-8"?>' + body).encode('utf-8')) is expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_path_errors_carry_the_storage_tag(
            self, provider, mock_time, generate_url_helper, monkeypatch):
        # ``_abort_chunked_upload`` parses the S3 error body to recognise
        # ``NoSuchUpload``.  The rule is that the tag, not the shape of the
        # exception, is what licenses reading a body as a storage response --
        # so the abort path has to go through ``_make_upload_request`` too.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100,
                                        headers={}, query_parameters=params)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>AccessDenied</Code><Message>nope</Message></Error>')
        aiohttpretty.register_uri('DELETE', abort_url, body=error_xml.encode('utf-8'), status=403)

        seen = []
        original = provider._log_abort_failure
        monkeypatch.setattr(provider, '_log_abort_failure',
                            lambda err, *a, **kw: (seen.append(err), original(err, *a, **kw))[1])

        await provider._abort_chunked_upload(path, upload_id)

        assert seen
        assert all(pd_provider._is_storage_response(err) for err in seen)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_failure_log_omits_raw_body(self, provider, mock_time,
                                                    generate_url_helper, caplog):
        # ``'{!r}'.format(UploadError(...))`` renders the entire message, and for
        # a dict message that is the storage's raw body serialised as JSON --
        # unbounded, and carrying Resource paths.  ``_translate_upload_error``
        # has a single bounded log for the body; this one must only identify the
        # failure by type and status.
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEUPLOADID'
        params = {'uploadId': upload_id}
        abort_url = generate_url_helper(key=path.full_path, method='DELETE', expires=100,
                                        headers={}, query_parameters=params)
        error_xml = ('<?xml version="1.0" encoding="UTF-8"?>'
                     '<Error><Code>AccessDenied</Code>'
                     '<Resource>/bucket/secret-key-name</Resource>'
                     '<Message>{}</Message></Error>').format('z' * 4096)
        aiohttpretty.register_uri('DELETE', abort_url, body=error_xml.encode('utf-8'),
                                  status=403)

        with caplog.at_level(logging.WARNING, logger=PROVIDER_LOGGER):
            aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aborted is False
        failures = [r.getMessage() for r in caplog.records
                    if r.name == PROVIDER_LOGGER and 'upload_id={}'.format(upload_id) in
                    r.getMessage() and 'has failed to abort' not in r.getMessage()]
        assert failures
        for logged in failures:
            # Enough to triage with...
            assert 'UploadError' in logged
            assert str(int(HTTPStatus.FORBIDDEN)) in logged
            assert upload_id in logged
            # ...including *which* S3 error it was.  Removing the raw body
            # without carrying the error code over left the log unable to
            # distinguish AccessDenied from InternalError, which is the first
            # thing anyone reading it needs to know.  The code is already
            # parsed one line earlier to test for NoSuchUpload.
            assert 'AccessDenied' in logged
            # ...and nothing of the body itself.
            assert 'secret-key-name' not in logged
            assert 'zzzz' not in logged
            assert len(logged) < 200

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

        # Mock the versions list response - list_object_versions is bucket-level, not object-level
        # Provider calls with Prefix, Delimiter, VersionIdMarker
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

        # Mock the boto3 delete_objects call
        mock_delete_response = {
            'Deleted': [{'Key': 'some-file', 'VersionId': 'null'}],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)
        # Verify delete_objects was called with correct parameters
        provider.bucket.delete_objects.assert_called_once()
        call_args = provider.bucket.delete_objects.call_args
        assert call_args[1]['Delete']['Objects'] == [{'Key': path.full_path, 'VersionId': 'null'}]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_confirm_delete(self, provider, version_metadata, mock_time, generate_url_helper):
        path = WaterButlerPath('/')

        # First call without confirm_delete - file deletion path
        # Mock request GET versions for file deletion - bucket-level operation
        query_params_file = {
            'Prefix': '',
            'Delimiter': '/',
            'VersionIdMarker': ''
        }
        versions_url_file = generate_url_helper(method='GET', expires=100, headers={}, query_parameters={'versions': '', **query_params_file})
        params_file = {'prefix': '', 'delimiter': '/', 'version-id-marker': '', 'versions': ''}
        
        # Second call with confirm_delete=1 - folder deletion path
        # Mock request GET versions for folder deletion (no Delimiter, no VersionIdMarker)
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

        # Mock _folder_prefix_exists check (list_objects_v2 with prefix stripped of trailing slash)
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

        # Mock the boto3 delete_objects call
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

        # Verify delete_objects was called once (for the second call with confirm_delete=1)
        assert provider.bucket.delete_objects.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_with_versions(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/folder-to-delete/')

        # Mock list versions response - bucket-level operation
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

        # Mock the boto3 delete_objects call
        mock_delete_response = {
            'Deleted': [
                {'Key': 'folder-to-delete/file1.txt', 'VersionId': '111'},
                {'Key': 'folder-to-delete/file1.txt', 'VersionId': '222'},
                {'Key': 'folder-to-delete/file2.txt', 'VersionId': '333'}
            ],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        # Mock _folder_prefix_exists check (list_objects_v2)
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

        # Verify list versions request was made
        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)

        # Verify delete_objects was called
        provider.bucket.delete_objects.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_truncated_response(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/large-folder/')

        # Mock first list versions response (truncated) - bucket-level operation
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

        # Mock second list versions response with pagination markers
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

        # Mock the boto3 delete_objects call
        mock_delete_response = {
            'Deleted': [
                {'Key': 'large-folder/file1.txt', 'VersionId': '111'},
                {'Key': 'large-folder/file2.txt', 'VersionId': '222'}
            ],
            'Errors': []
        }
        provider.bucket.delete_objects = mock.Mock(return_value=mock_delete_response)

        # Mock _folder_prefix_exists check (list_objects_v2)
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

        # Verify both list versions requests were made
        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params1)
        assert aiohttpretty.has_call(method='GET', uri=versions_url2, params=params2)

        # Verify delete_objects was called once
        provider.bucket.delete_objects.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_not_found(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/not-found-folder/')
        prefix = path.full_path.lstrip('/')  # 'not-found-folder/'

        # Mock get_full_revision response with empty versions and delete_markers - bucket-level operation
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

        # Verify the request was made
        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=versions_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_delete_error(self, provider, mock_time, generate_url_helper):
        path = WaterButlerPath('/error-folder/')

        # Mock list versions response - bucket-level operation
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

        # Mock failed delete_objects response
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

        # Verify both requests were made
        assert aiohttpretty.has_call(method='GET', uri=versions_url, params=params)
        provider.bucket.delete_objects.assert_called_once()


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
        # Provider uses list_objects_v2 which doesn't take a key parameter, only query params
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
        # 1 CommonPrefixes + 1 Contents + 1 next_token string = 3 items
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        # Last item is the next_token string for pagination
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

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_must_have_slash(self, provider, folder_item_metadata, mock_time):
    #     with pytest.raises(exceptions.InvalidPathError):
    #         await provider.metadata('')

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

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
