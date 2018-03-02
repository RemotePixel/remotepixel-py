"""remotepixel.srtm_mosaic"""

import zlib
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


def create(tiles):
    """
    """
    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        responses = executor.map(worker, tiles)

    with contextlib.ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(tile)) for tile in responses if tile]
        dest, output_transform = merge(sources, nodata=-32767)

    meta = {
        'driver': 'GTiff',
        'count': 1,
        'dtype': np.int16,
        'nodata': -32767,
        'height': dest.shape[1],
        'width': dest.shape[2],
        'compress': 'DEFLATE',
        'crs': 'epsg:4326',
        'transform': output_transform}

    memfile = MemoryFile()
    with memfile.open(**meta) as dataset:
        dataset.write(dest)

    return memfile
