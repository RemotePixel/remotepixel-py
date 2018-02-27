
import os

from mock import patch

from remotepixel import s2_ndvi

sentinel_scene = 'S2A_tile_20170729_19UDP_0'
sentinel_bucket = os.path.join(os.path.dirname(__file__), 'fixtures', 'sentinel-s2-l1c')
sentinel_path = os.path.join(sentinel_bucket, 'tiles/19/U/DP/2017/7/29/0/')


@patch('remotepixel.utils.sentinel2_get_info')
def test_point_valid(sentinel2_get_info, monkeypatch):
    """Should work as expected (read data, calculate NDVI and return json info)
    """
    monkeypatch.setattr(s2_ndvi, 'SENTINEL_BUCKET', sentinel_bucket)
    sentinel2_get_info.return_value = {
        'cloud_coverage': 5.01,
        'sat': 'S2B'}

    expression = '(b08 - b04) / (b08 + b04)'

    coords = [-69.6140202938876, 48.25520824803732]
    expectedContent = {
        "date": '2017-07-29',
        "sat": 'S2B',
        "scene": sentinel_scene,
        "cloud": 5.01,
        "ndvi": 0.15250335699213505}

    assert s2_ndvi.point(sentinel_scene, coords, expression) == expectedContent


def test_area_valid(monkeypatch):
    """Should work as expected (read data, calculate NDVI and return img)
    """
    monkeypatch.setattr(s2_ndvi, 'SENTINEL_BUCKET', sentinel_bucket)

    expression = '(b08 - b04) / (b08 + b04)'
    bbox = [-68.90625, 47.98992166741417, -67.5, 48.92249926375824]

    assert s2_ndvi.area(sentinel_scene, bbox, expression)
