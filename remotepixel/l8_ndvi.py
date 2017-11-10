"""remotepixel.l8_ndvi"""

import base64
from io import BytesIO

import numpy as np
from PIL import Image

import rasterio as rio
from rasterio import warp
from rasterio.enums import Resampling
from rio_toa.reflectance import reflectance

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def point(scene, coord):
    """
    """

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

    multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_4']
    add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_4']
    band_address = f'{landsat_address}_B4.TIF'
    with rio.open(band_address) as band:
        lon_srs, lat_srs = warp.transform('EPSG:4326', band.crs, [coord[0]], [coord[1]])
        b4 = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]
        b4 = reflectance(b4, multi_reflect, add_reflect, sun_elev, src_nodata=0)[0]

    multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_5']
    add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_5']
    band_address = f'{landsat_address}_B5.TIF'
    with rio.open(band_address) as band:
        lon_srs, lat_srs = warp.transform('EPSG:4326', band.crs, [coord[0]], [coord[1]])
        b5 = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]
        b5 = reflectance(b5, multi_reflect, add_reflect, sun_elev, src_nodata=0)[0]

    ratio = float(np.nan_to_num((b5 - b4) / (b5 + b4)) if (b4 * b5) > 0 else 0.)

    out = {
        'ndvi': ratio,
        'date': scene_params['date'],
        'cloud': meta_data['IMAGE_ATTRIBUTES']['CLOUD_COVER']}

    return out


def area(scene, bbox):
    """
    """

    max_width = 512
    max_height = 512

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

    multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_4']
    add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_4']
    band_address = f'{landsat_address}_B4.TIF'
    with rio.open(band_address) as band:
        crs_bounds = warp.transform_bounds('EPSG:4326', band.crs, *bbox)
        window = band.window(*crs_bounds)

        width = round(window.width) if window.width < max_width else max_width
        height = round(window.height) if window.height < max_height else max_height

        b4 = band.read(window=window, out_shape=(height, width), indexes=1, resampling=Resampling.bilinear, boundless=True)
        b4 = reflectance(b4, multi_reflect, add_reflect, sun_elev, src_nodata=0)

    multi_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_5']
    add_reflect = meta_data['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_5']
    band_address = f'{landsat_address}_B5.TIF'
    with rio.open(band_address) as band:
        crs_bounds = warp.transform_bounds('EPSG:4326', band.crs, *bbox)
        window = band.window(*crs_bounds)

        width = round(window.width) if window.width < max_width else max_width
        height = round(window.height) if window.height < max_height else max_height

        b5 = band.read(window=window, out_shape=(height, width), indexes=1, resampling=Resampling.bilinear, boundless=True)
        b5 = reflectance(b5, multi_reflect, add_reflect, sun_elev, src_nodata=0)

    ratio = np.where((b5 * b4) > 0, np.nan_to_num((b5 - b4) / (b5 + b4)), -9999)
    ratio = np.where(ratio > -9999, utils.linear_rescale(ratio, in_range=[-1, 1], out_range=[1, 255]), 0).astype(np.uint8)

    cmap = list(np.array(utils.get_colormap()).flatten())
    img = Image.fromarray(ratio, 'P')
    img.putpalette(cmap)
    img = img.convert('RGB')

    sio = BytesIO()
    img.save(sio, 'jpeg', subsampling=0, quality=100)
    sio.seek(0)

    return base64.b64encode(sio.getvalue()).decode()
