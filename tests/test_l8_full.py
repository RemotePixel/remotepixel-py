
import os
import pytest
from mock import patch

from rio_toa import toa_utils
from remotepixel import l8_full

landsat_scene_c1 = 'LC08_L1TP_016037_20170813_20170814_01_RT'
landsat_bucket = os.path.join(os.path.dirname(__file__), 'fixtures', 'landsat-pds')

landsat_path = os.path.join(landsat_bucket, 'c1', 'L8', '016', '037', landsat_scene_c1, landsat_scene_c1)
with open('{}_MTL.txt'.format(landsat_path), 'r') as f:
    landsat_meta = toa_utils._parse_mtl_txt(f.read())


@patch('remotepixel.l8_full.boto3.client')
@patch('remotepixel.utils.landsat_get_mtl')
def test_create_bands(landsat_get_mtl, client, monkeypatch):
    """Should work as expected (read data, create RGB and upload to S3)
    """

    monkeypatch.setattr(l8_full, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    bands = [5, 4, 3]
    assert l8_full.create(landsat_scene_c1, bucket, bands=bands)


@patch('remotepixel.l8_full.boto3.client')
@patch('remotepixel.utils.landsat_get_mtl')
def test_create_expression(landsat_get_mtl, client, monkeypatch):
    """Should work as expected (read data, create NDVI and upload to S3)
    """

    expression = '(b5 - b4) / (b5 + b4)'

    monkeypatch.setattr(l8_full, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    assert l8_full.create(landsat_scene_c1, bucket, expression=expression)


@patch('remotepixel.l8_full.boto3.client')
@patch('remotepixel.utils.landsat_get_mtl')
def test_create_lessBand(landsat_get_mtl, client, monkeypatch):
    """
    """

    monkeypatch.setattr(l8_full, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    bands = [5, 4]
    with pytest.raises(Exception):
        l8_full.create(landsat_scene_c1, bucket, bands=bands)


@patch('remotepixel.l8_full.boto3.client')
@patch('remotepixel.utils.landsat_get_mtl')
def test_create_noExpreBands(landsat_get_mtl, client, monkeypatch):
    """
    """

    monkeypatch.setattr(l8_full, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta
    client.return_value.upload_fileobj.return_value = True

    bucket = 'my-bucket'
    with pytest.raises(Exception):
        l8_full.create(landsat_scene_c1, bucket)
