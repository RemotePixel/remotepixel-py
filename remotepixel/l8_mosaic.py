"""remotepixel.l8_mosaic"""

import os
import json
from functools import partial
from concurrent import futures
from datetime import datetime, timedelta

import boto3
import numpy as np

import rasterio as rio
from rasterio.merge import merge
from rasterio.io import MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from rasterio.warp import transform_bounds, calculate_default_transform
from rio_toa.reflectance import reflectance

from remotepixel import utils

LANDSAT_BUCKET = 's3://landsat-pds'


def get_scene(scene, bands):
    """
    """

    try:
        scene_params = utils.landsat_parse_scene_id(scene)
        meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
        landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

        bqa = f'{landsat_address}_BQA.TIF'
        with rio.open(bqa) as src:
            ovr = src.overviews(1)
            ovr_width = int(src.width / ovr[0])
            ovr_height = int(src.height / ovr[0])

            dst_affine, width, height = calculate_default_transform(src.crs, 'epsg:3857', ovr_width, ovr_height, *src.bounds)

        outpath = f'/tmp/{scene}.tif'
        with rio.open(outpath,
                      'w',
                      driver='GTiff',
                      count=3,
                      dtype=np.uint8,
                      nodata=0,
                      height=height,
                      width=width,
                      crs='epsg:3857',
                      transform=dst_affine) as dataset:

            sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

            for b in range(len(bands)):
                band_address = f'{landsat_address}_B{bands[b]}.TIF'

                with rio.open(band_address) as src:
                    with WarpedVRT(src, dst_crs='EPSG:3857', resampling=Resampling.bilinear, src_nodata=0, dst_nodata=0) as vrt:
                        matrix = vrt.read(indexes=1, out_shape=(height, width))

                multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_{bands[b]}']
                add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_{bands[b]}']
                minref = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MINIMUM_BAND_{bands[b]}'] * 10000
                maxref = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MAXIMUM_BAND_{bands[b]}'] * 10000

                matrix = reflectance(matrix, multi_reflect, add_reflect, sun_elev, src_nodata=0) * 10000
                matrix = np.where(matrix > 0, utils.linear_rescale(matrix, in_range=[int(minref), int(maxref)], out_range=[1, 255]), 0).astype(np.uint8)

                mask = np.ma.masked_values(matrix, 0)
                s = np.ma.notmasked_contiguous(mask)
                mask = None
                matrix = matrix.ravel()
                for sl in s:
                    matrix[sl.start: sl.start + 5] = 0
                    matrix[sl.stop - 5:sl.stop] = 0
                matrix = matrix.reshape((height, width))

                dataset.write(matrix, indexes=b+1)

        return outpath

    except:
        return None


def create(scenes, uuid, bucket, bands=[4, 3, 2]):
    """
    """

    _worker = partial(get_scene, bands=bands)
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        allScenes = executor.map(_worker, scenes)

    sources = [rio.open(x) for x in allScenes if x]
    dest, output_transform = merge(sources, nodata=0)

    for tmp in allScenes:
        if tmp:
            os.remove(tmp)

    with MemoryFile() as memfile:
        with memfile.open(driver='GTiff',
                          count=3,
                          dtype=np.uint8,
                          nodata=0,
                          height=dest.shape[1],
                          width=dest.shape[2],
                          compress='JPEG',
                          crs='epsg:3857',
                          transform=output_transform) as dataset:

            dataset.write(dest)
            wgs_bounds = transform_bounds(
                *[dataset.crs, 'epsg:4326'] +
                list(dataset.bounds), densify_pts=21)

        client = boto3.client('s3')
        expiration = datetime.now() + timedelta(days=7)

        client.put_object(
            ACL='public-read',
            Bucket=os.environ.get('OUTPUT_BUCKET'),
            Key=f'data/mosaic/{uuid}_mosaic.tif',
            Expires=expiration,
            Body=memfile,
            ContentType='image/tiff')

        meta = {
            'id': uuid,
            'mosaic': '{}_mosaic.tif'.format(uuid),
            'coordinates': {
                'north': wgs_bounds[3],
                'west': wgs_bounds[0],
                'south': wgs_bounds[1],
                'east': wgs_bounds[2],
                'Proj': 'EPSG:4326'}}

        client.put_object(
            ACL='public-read',
            Bucket=bucket,
            Key=f'data/mosaic/{uuid}.json',
            Expires=expiration,
            Body=json.dumps(meta),
            ContentType='application/json')

    return True
