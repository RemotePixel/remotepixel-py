"""remotepixel.s2_full"""

from concurrent import futures

import boto3
import numpy as np

import rasterio as rio
from rasterio.io import MemoryFile

from remotepixel import utils

SENTINEL_BUCKET = 's3://sentinel-s2-l1c'


def worker(band_address):
    """
    """
    with rio.open(band_address) as src:
        data = src.read(indexes=1)
        imgRange = np.percentile(data[data > 0], (2, 98)).tolist()
        return np.where(data > 0, utils.linear_rescale(data, in_range=imgRange, out_range=[1, 255]), 0).astype(np.uint8)


def create(scene, out_bucket, bands=['04', '03', '02']):
    """
    """

    scene_params = utils.sentinel_parse_scene_id(scene)
    sentinel_address = f'{SENTINEL_BUCKET}/{scene_params["key"]}'

    band_address = f'{sentinel_address}/B{bands[0]}.jp2'
    with rio.open(band_address) as src:
        meta = src.meta

    meta.update(driver='GTiff',
                nodata=0,
                count=3,
                dtype=np.uint8,
                interleave='pixel',
                PHOTOMETRIC='YCbCr',
                compress='JPEG')

    addresses = [f'{sentinel_address}/B{band}.jp2' for band in bands]

    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:
            with futures.ThreadPoolExecutor(max_workers=3) as executor:
                dataset.write(np.stack(list(executor.map(worker, addresses))))

        str_band = ''.join(map(str, bands))
        key = f'data/sentinel2/{scene}_B{str_band}.tif'

        client = boto3.client('s3')
        client.upload_fileobj(memfile, out_bucket, key,
                              ExtraArgs={
                                    'ACL': 'public-read',
                                    'ContentType': 'image/tiff'})

    return True
