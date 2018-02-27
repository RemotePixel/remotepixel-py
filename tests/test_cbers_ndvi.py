
import os

from remotepixel import cbers_ndvi

CBERS_SCENE = 'CBERS_4_MUX_20171121_057_094_L2'
CBERS_BUCKET = os.path.join(os.path.dirname(__file__), 'fixtures', 'cbers-pds')
CBERS_PATH = os.path.join(CBERS_BUCKET, 'CBERS4/MUX/057/094/CBERS_4_MUX_20171121_057_094_L2/')


def test_point_valid(monkeypatch):
    """Should work as expected (read data, calculate NDVI and return json info)
    """
    monkeypatch.setattr(cbers_ndvi, 'CBERS_BUCKET', CBERS_BUCKET)

    expression = '(b8 - b7) / (b8 + b7)'
    coords = [53.9097, 5.3674]
    expectedContent = {
        "date": '2017-11-21',
        "ndvi": -0.1320754716981132}

    assert cbers_ndvi.point(CBERS_SCENE, coords, expression) == expectedContent


def test_area_valid(monkeypatch):
    """Should work as expected (read data, calculate NDVI and return img)
    """
    monkeypatch.setattr(cbers_ndvi, 'CBERS_BUCKET', CBERS_BUCKET)

    expression = '(b8 - b7) / (b8 + b7)'
    bbox = [53.0859375, 5.266007882805496, 53.4375, 5.615985819155334]

    assert cbers_ndvi.area(CBERS_SCENE, bbox, expression)
