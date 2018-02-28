"""remotepixel.srtm_mosaic.py"""

import uuid
import zlib
import boto3
import contextlib
from concurrent import futures

import numpy as np

import rasterio
from rasterio.merge import merge
from rasterio.io import MemoryFile

from remotepixel import aws

SRTM_BUCKET = 'elevation-tiles-prod'


def worker(tile):
    """
    """
    try:
        outpath = f'/tmp/{tile}.hgt'
        key = f'skadi/{tile[0:3]}/{tile}.hgt.gz'
        with open(outpath, 'wb') as f:
            f.write(zlib.decompress(aws.get_object(SRTM_BUCKET, key), zlib.MAX_WBITS | 16))
        return outpath
    except:
        return ''


def create(tiles, out_bucket, uid=None):
    """
    """
    if not uid:
        uid = str(uuid.uuid1())

    if len(tiles) > 8:
        return False

    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        responses = list(executor.map(worker, tiles))

    with contextlib.ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(tile)) for tile in responses if tile]
        dest, output_transform = merge(sources, nodata=-32767)

        with MemoryFile() as memfile:
            with memfile.open(driver='GTiff',
                              count=1,
                              dtype=np.int16,
                              nodata=-32767,
                              height=dest.shape[1],
                              width=dest.shape[2],
                              compress='DEFLATE',
                              crs='epsg:4326',
                              transform=output_transform) as dataset:
                                dataset.write(dest)

            params = {
                'ACL': 'public-read',
                'Metadata': {
                    'uuid': uid},
                'ContentType': 'image/tiff'}

            key = f'data/srtm/{uid}.tif'
            client = boto3.client('s3')
            client.upload_fileobj(memfile, out_bucket, key, ExtraArgs=params)

    return key
