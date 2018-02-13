
import os
import pytest
from mock import patch

from remotepixel import cbers_full

CBERS_SCENE = 'CBERS_4_MUX_20171121_057_094_L2'
CBERS_BUCKET = os.path.join(os.path.dirname(__file__), 'fixtures', 'cbers-pds')

@patch('remotepixel.cbers_full.boto3.client')
def test_create_bands(client, monkeypatch):
    """Should work as expected (read data, create RGB and upload to S3)
    """

    monkeypatch.setattr(cbers_full, 'CBERS_BUCKET', CBERS_BUCKET)
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    bands = [7, 6, 5]
    assert cbers_full.create(CBERS_SCENE, bucket, bands=bands)


@patch('remotepixel.cbers_full.boto3.client')
def test_create_expression(client, monkeypatch):
    """Should work as expected (read data, create NDVI and upload to S3)
    """

    expression = '(b8 - b7) / (b8 + b7)'

    monkeypatch.setattr(cbers_full, 'CBERS_BUCKET', CBERS_BUCKET)
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    assert cbers_full.create(CBERS_SCENE, bucket, expression=expression)


@patch('remotepixel.cbers_full.boto3.client')
def test_create_lessBand(client, monkeypatch):
    """
    """

    monkeypatch.setattr(cbers_full, 'CBERS_BUCKET', CBERS_BUCKET)
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    bands = [7, 6]
    with pytest.raises(Exception):
        cbers_full.create(CBERS_SCENE, bucket, bands=bands)


@patch('remotepixel.cbers_full.boto3.client')
def test_create_noExpreBands(client, monkeypatch):
    """
    """

    monkeypatch.setattr(cbers_full, 'CBERS_BUCKET', CBERS_BUCKET)
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    with pytest.raises(Exception):
        cbers_full.create(CBERS_SCENE, bucket)
