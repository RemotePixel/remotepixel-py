"""remotepixel.l8_mosaic"""

import contextlib
from functools import partial
from concurrent import futures

import numpy as np
# import numexpr as ne

import rasterio
from rasterio.merge import merge
from rasterio.io import MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from rasterio.warp import transform_bounds, calculate_default_transform
from rio_toa.reflectance import reflectance

from remotepixel import utils

LANDSAT_BUCKET = 's3://landsat-pds'


def worker(scene, bands):
    """
    """

    try:
        scene_params = utils.landsat_parse_scene_id(scene)
        meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
        landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

        bqa = f'{landsat_address}_BQA.TIF'
        with rasterio.open(bqa) as src:
            ovr = src.overviews(1)
            ovr_width = int(src.width / ovr[0])
            ovr_height = int(src.height / ovr[0])
            dst_affine, width, height = calculate_default_transform(src.crs, 'epsg:3857', ovr_width, ovr_height, *src.bounds)

            meta = {
                'driver': 'GTiff',
                'count': 3,
                'dtype': np.uint8,
                'nodata': 0,
                'height': height,
                'width': width,
                'compress': 'DEFLATE',
                'crs': 'epsg:3857',
                'transform': dst_affine}

        outpath = f'/tmp/{scene}.tif'
        with rasterio.open(outpath, 'w', **meta) as dataset:

            sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

            for idx, b in enumerate(bands):
                with rasterio.open(f'{landsat_address}_B{b}.TIF') as src:
                    with WarpedVRT(src, dst_crs='EPSG:3857',
                                   resampling=Resampling.bilinear,
                                   src_nodata=0, dst_nodata=0) as vrt:
                        matrix = vrt.read(indexes=1, out_shape=(height, width))

                multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_{b}']
                add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_{b}']
                matrix = reflectance(matrix, multi_reflect, add_reflect, sun_elev, src_nodata=0) * 10000

                minref = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MINIMUM_BAND_{b}'] * 10000
                maxref = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MAXIMUM_BAND_{b}'] * 10000
                matrix = np.where(matrix > 0,
                                  utils.linear_rescale(matrix, in_range=[int(minref), int(maxref)], out_range=[1, 255]),
                                  0).astype(np.uint8)

                mask = np.ma.masked_values(matrix, 0)
                s = np.ma.notmasked_contiguous(mask)
                matrix = matrix.ravel()
                for sl in s:
                    matrix[sl.start: sl.start + 5] = 0
                    matrix[sl.stop - 5:sl.stop] = 0
                matrix = matrix.reshape((height, width))

                dataset.write(matrix, indexes=idx+1)

        return outpath
    except:
        return None


def create(scenes, bands=[4, 3, 2]):
    """
    """

    _worker = partial(worker, bands=bands)
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        responses = executor.map(_worker, scenes)

    with contextlib.ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(scene)) for scene in responses if scene]
        dest, output_transform = merge(sources, nodata=0)

        meta = {
            'driver': 'GTiff',
            'count': 3,
            'dtype': np.uint8,
            'nodata': 0,
            'height': dest.shape[1],
            'width': dest.shape[2],
            'compress': 'JPEG',
            'crs': 'epsg:3857',
            'transform': output_transform}

        memfile = MemoryFile()
        with memfile.open(**meta) as dataset:
            dataset.write(dest)
            wgs_bounds = transform_bounds(
                *[dataset.crs, 'epsg:4326'] + list(dataset.bounds), densify_pts=21)

    return memfile, wgs_bounds
