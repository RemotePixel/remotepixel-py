"""remotepixel.l8_ovr"""

import base64
from io import BytesIO
from functools import partial
from concurrent import futures

import numpy as np
from PIL import Image

from rio_toa.reflectance import reflectance

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def worker(band, landsat_address, meta, ovr_size, ndvi):
    """
    """

    address = f'{landsat_address}_B{band}.TIF'

    sun_elev = meta['IMAGE_ATTRIBUTES']['SUN_ELEVATION']
    multi_reflect = meta['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_{band}']
    add_reflect = meta['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_{band}']

    matrix = utils.get_overview(address, ovr_size)
    matrix = reflectance(matrix, multi_reflect, add_reflect, sun_elev, src_nodata=0)
    if not ndvi:
        imgRange = np.percentile(matrix[matrix > 0], (2, 98)).tolist()
        matrix = np.where(matrix > 0, utils.linear_rescale(matrix, in_range=imgRange, out_range=[1, 255]), 0).astype(np.uint8)

    return matrix


def create(scene, bands=[4, 3, 2], img_format='jpeg', ovrSize=512):
    """
    """

    if img_format not in ['png', 'jpeg']:
        raise UserWarning(f'Invalid {img_format} extension')

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    _worker = partial(worker, landsat_address=landsat_address, meta=meta_data, ovr_size=ovrSize, ndvi=False)
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        out = np.stack(list(executor.map(_worker, bands)))

    mask_shape = (1,) + out.shape[-2:]
    mask = np.full(mask_shape, 255, dtype=np.uint8)
    mask[0] = np.all(np.dstack(out) != 0, axis=2).astype(np.uint8) * 255
    out = np.concatenate((out, mask))

    img = Image.fromarray(np.stack(out, axis=2))

    sio = BytesIO()

    if img_format == 'jpeg':
        img = img.convert('RGB')
        img.save(sio, 'jpeg', subsampling=0, quality=100)
    else:
        img.save(sio, 'png', compress_level=0)

    sio.seek(0)

    return base64.b64encode(sio.getvalue()).decode()


def create_ndvi(scene, img_format='jpeg', ovrSize=512):
    """
    """

    if img_format not in ['png', 'jpeg']:
        raise UserWarning(f'Invalid {img_format} extension')

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    bands = [4, 5]

    _worker = partial(worker, landsat_address=landsat_address, meta=meta_data, ovr_size=ovrSize, ndvi=True)
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        out = np.stack(list(executor.map(_worker, bands)))

    ratio = np.where((out[1] * out[0]) > 0, np.nan_to_num((out[1] - out[0]) / (out[1] + out[0])), -9999)
    ratio = np.where(ratio > -9999, utils.linear_rescale(ratio, in_range=[-1, 1], out_range=[1, 255]), 0).astype(np.uint8)

    cmap = list(np.array(utils.get_colormap()).flatten())
    img = Image.fromarray(ratio, 'P')
    img.putpalette(cmap)

    sio = BytesIO()
    if img_format == 'jpeg':
        img = img.convert('RGB')
        img.save(sio, 'jpeg', subsampling=0, quality=100)
    else:
        img.save(sio, 'png', compress_level=0)

    sio.seek(0)

    return base64.b64encode(sio.getvalue()).decode()
