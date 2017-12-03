"""remotepixel.s2_full"""

from concurrent import futures

import boto3
import numpy as np

import rasterio as rio
from rasterio.io import MemoryFile

from remotepixel import utils

SENTINEL_BUCKET = 's3://sentinel-s2-l1c'

################################################################################

# This code won't run on AWS Lambda

# TO DO: handle multi resolution

################################################################################

# https://en.wikipedia.org/wiki/Sentinel-2
band_info = {
    '01': {'res': 60, 'wavelenght': 0.443, 'name': 'Coastal aerosol'},
    '02': {'res': 10, 'wavelenght': 0.490, 'name': 'Blue'},
    '03': {'res': 10, 'wavelenght': 0.560, 'name': 'Green'},
    '04': {'res': 10, 'wavelenght': 0.665, 'name': 'Red'},
    '05': {'res': 20, 'wavelenght': 0.705, 'name': 'Vegetation Red Edge'},
    '06': {'res': 20, 'wavelenght': 0.740, 'name': 'Vegetation Red Edge'},
    '07': {'res': 20, 'wavelenght': 0.783, 'name': 'Vegetation Red Edge'},
    '08': {'res': 10, 'wavelenght': 0.842, 'name': 'NIR'},
    '8A': {'res': 20, 'wavelenght': 0.865, 'name': 'Vegetation Red Edge'},
    '09': {'res': 60, 'wavelenght': 0.945, 'name': 'Water vapour'},
    '10': {'res': 60, 'wavelenght': 1.375, 'name': 'SWIR'},
    '11': {'res': 20, 'wavelenght': 1.610, 'name': 'SWIR'},
    '12': {'res': 20, 'wavelenght': 2.190, 'name': 'SWIR'}}


def worker(band_address):
    """
    """
    with rio.open(band_address) as src:
        return src.read(indexes=1)


def create(scene, bucket, bands=['04', '03', '02']):
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
                interleave='pixel',
                PHOTOMETRIC='RGB',
                compress='DEFLATE')

    addresses = [f'{sentinel_address}/B{band}.jp2' for band in bands]

    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:
            with futures.ThreadPoolExecutor(max_workers=3) as executor:
                dataset.write(np.stack(list(executor.map(worker, addresses))))

        str_band = ''.join(map(str, bands))
        key = f'data/s2/{scene}_B{str_band}.tif'

        client = boto3.client('s3')
        client.upload_fileobj(memfile, bucket, key,
                              ExtraArgs={
                                    'ACL': 'public-read',
                                    'ContentType': 'image/tiff'})

    return True
