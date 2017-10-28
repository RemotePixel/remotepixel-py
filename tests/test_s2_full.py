
import os

from mock import patch

from remotepixel import s2_full

sentinel_scene = 'S2A_tile_20170729_19UDP_0'
sentinel_bucket = os.path.join(os.path.dirname(__file__), 'fixtures', 'sentinel-s2-l1c')
sentinel_path = os.path.join(sentinel_bucket, 'tiles/19/U/DP/2017/7/29/0/')


def test_worker_valid():
    """
    Should work as expected (read data, and rescale to int)
    """

    address = f'{sentinel_path}/B04.jp2'

    assert s2_full.worker(address).shape == (122, 122)


@patch('remotepixel.s2_full.boto3.client')
def test_create_valid(client, monkeypatch):
    """
    Should work as expected (read r,g,b bands and create JPEG image)
    """

    monkeypatch.setattr(s2_full, 'SENTINEL_BUCKET', sentinel_bucket)
    client.return_value.put_object.return_value = True

    bucket = 'my-bucket'

    assert s2_full.create(sentinel_scene, bucket)


@patch('remotepixel.s2_full.boto3.client')
def test_create_validBands(client, monkeypatch):
    """
    Should work as expected (read data, create RGB and upload to S3)
    """

    monkeypatch.setattr(s2_full, 'SENTINEL_BUCKET', sentinel_bucket)
    client.return_value.put_object.return_value = True

    bucket = 'my-bucket'
    bands = ['08', '04', '03']
    assert s2_full.create(sentinel_scene, bucket, bands)
