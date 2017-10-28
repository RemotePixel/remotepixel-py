"""remotepixel.l8_full"""

from datetime import datetime, timedelta

import boto3
import numpy as np

import rasterio as rio
from rasterio.io import MemoryFile
from rio_toa.reflectance import reflectance

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def create(scene, bucket, bands=[4, 3, 2]):
    """
    """
    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    bqa = f'{landsat_address}_BQA.TIF'
    with rio.open(bqa) as src:
        meta = src.meta
        wind = [w for ij, w in src.block_windows(1)]
        meta.update(nodata=0, count=3, interleave='pixel', PHOTOMETRIC='RGB', compress=None)

    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:

            sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

            for b in range(len(bands)):
                band_address = f'{landsat_address}_B{bands[b]}.TIF'

                multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_{bands[b]}']
                add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_{bands[b]}']

                with rio.open(band_address) as src:
                    for window in wind:
                        matrix = src.read(window=window, boundless=True, indexes=1)
                        result = 10000 * reflectance(matrix, multi_reflect, add_reflect, sun_elev, src_nodata=0)
                        dataset.write(result.astype(np.uint16), window=window, indexes=b+1)

        client = boto3.client('s3')
        str_band = ''.join(map(str, bands))
        key = f'data/landsat/{scene}_B{str_band}.tif'
        expiration = datetime.now() + timedelta(days=15)

        client.put_object(
            ACL='public-read',
            Bucket=bucket,
            Key=key,
            Expires=expiration,
            Body=memfile,
            ContentType='image/tiff')

    return True


def create_ndvi(scene, bucket):
    """
    """

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    bqa = f'{landsat_address}_BQA.TIF'
    with rio.open(bqa) as src:
        meta = src.meta
        wind = [w for ij, w in src.block_windows(1)]
        meta.update(nodata=-9999, count=1, interleave='pixel', compress=None, dtype=np.float32)

    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:

            sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

            band_address_b = f'{landsat_address}_B4.TIF'
            multi_reflect4 = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_4']
            add_reflect4 = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_4']

            band_address_n = f'{landsat_address}_B5.TIF'
            multi_reflect5 = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_5']
            add_reflect5 = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_5']

            with rio.open(band_address_b) as b4:
                with rio.open(band_address_n) as b5:
                    for window in wind:
                        matrix = b4.read(window=window, boundless=True, indexes=1)
                        b4_data = reflectance(matrix, multi_reflect4, add_reflect4, sun_elev, src_nodata=0)
                        matrix = b5.read(window=window, boundless=True, indexes=1)
                        b5_data = reflectance(matrix, multi_reflect5, add_reflect5, sun_elev, src_nodata=0)
                        ratio = np.where((b5_data * b4_data) > 0, np.nan_to_num((b5_data - b4_data) / (b5_data + b4_data)), -9999)
                        dataset.write(ratio, window=window, indexes=1)

        client = boto3.client('s3')
        key = f'data/landsat/{scene}_NDVI.tif'
        expiration = datetime.now() + timedelta(days=15)

        client.put_object(
            ACL='public-read',
            Bucket=bucket,
            Key=key,
            Expires=expiration,
            Body=memfile,
            ContentType='image/tiff')

    return True
