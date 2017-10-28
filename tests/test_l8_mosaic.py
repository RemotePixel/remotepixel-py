
import os

from mock import patch

from rio_toa import toa_utils
from remotepixel import l8_mosaic

landsat_scene_c1 = 'LC08_L1TP_016037_20170813_20170814_01_RT'
landsat_bucket = os.path.join(os.path.dirname(__file__), 'fixtures', 'landsat-pds')

landsat_path = os.path.join(landsat_bucket, 'c1', 'L8', '016', '037', landsat_scene_c1, landsat_scene_c1)
with open(f'{landsat_path}_MTL.txt', 'r') as f:
    landsat_meta = toa_utils._parse_mtl_txt(f.read())


@patch('remotepixel.utils.landsat_get_mtl')
def test_get_scene_valid(landsat_get_mtl, monkeypatch):
    """
    Should work as expected (read data, proccess to TOA and rescale to int)
    """

    monkeypatch.setattr(l8_mosaic, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta

    expectedContent = '/tmp/LC08_L1TP_016037_20170813_20170814_01_RT.tif'
    assert l8_mosaic.get_scene(landsat_scene_c1, [4, 3, 2]) == expectedContent


@patch('remotepixel.l8_mosaic.boto3.client')
@patch('remotepixel.utils.landsat_get_mtl')
def test_create_valid(landsat_get_mtl, client, monkeypatch):
    """
    Should work as expected (read data, proccess to TOA and rescale to int)
    """

    monkeypatch.setattr(l8_mosaic, 'LANDSAT_BUCKET', landsat_bucket)
    landsat_get_mtl.return_value = landsat_meta
    client.return_value.put_object.return_value = True

    assert l8_mosaic.create([landsat_scene_c1], '000000', 'my-bucket')
