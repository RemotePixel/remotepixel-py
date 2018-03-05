"""remotepixel.l8_ovr"""

import re
import base64
from io import BytesIO
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

from PIL import Image

from rasterio.plot import reshape_as_image
from rio_toa.reflectance import reflectance

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def worker(band, landsat_address, meta, ovr_size):
    """
    """

    address = f'{landsat_address}_B{band}.TIF'

    sun_elev = meta['IMAGE_ATTRIBUTES']['SUN_ELEVATION']
    multi_reflect = meta['RADIOMETRIC_RESCALING'][f'REFLECTANCE_MULT_BAND_{band}']
    add_reflect = meta['RADIOMETRIC_RESCALING'][f'REFLECTANCE_ADD_BAND_{band}']

    matrix = utils.get_overview(address, ovr_size)
    return reflectance(matrix, multi_reflect, add_reflect, sun_elev, src_nodata=0)


def create(scene, bands=None, expression=None, expression_range=[-1, 1], img_format='jpeg', ovrSize=512):
    """
    """

    if img_format not in ['png', 'jpeg']:
        raise UserWarning(f'Invalid {img_format} extension')

    if not expression and not bands:
        raise Exception('Expression or Bands must be provided')

    if bands:
        nb_bands = len(bands)
        if nb_bands != 3:
            raise Exception('RGB combination only')

    if expression:
        bands = tuple(set(re.findall(r'b(?P<bands>[0-9]{1,2})', expression)))
        rgb = expression.split(',')
        nb_bands = len(rgb)

    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    _worker = partial(worker, landsat_address=landsat_address, meta=meta_data, ovr_size=ovrSize)
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        data = np.concatenate(list(executor.map(_worker, bands)))
        mask = np.all(data != 0, axis=0).astype(np.uint8) * 255

        if expression:
            ctx = {}
            for bdx, b in enumerate(bands):
                ctx['b{}'.format(b)] = data[bdx]
            data = np.array([np.nan_to_num(ne.evaluate(bloc.strip(), local_dict=ctx)) for bloc in rgb])

        for band in range(data.shape[0]):
            imgRange = expression_range if expression else np.percentile(data[band][mask > 0], (2, 98)).tolist()
            data[band] = np.where(mask, utils.linear_rescale(data[band], in_range=imgRange, out_range=[0, 255]), 0)

    data = data.squeeze()
    if len(data.shape) >= 3:
        data = reshape_as_image(data.astype(np.uint8))
        img = Image.fromarray(data, 'RGB')
    else:
        cmap = list(np.array(utils.get_colormap()).flatten())
        img = Image.fromarray(data.astype(np.uint8), 'L')
        img.putpalette(cmap)
        img = img.convert('RGB')

    sio = BytesIO()
    if img_format == 'jpeg':
        img.save(sio, 'jpeg', quality=95)
    else:
        mask_img = Image.fromarray(mask)
        img.putalpha(mask_img)
        img.save(sio, 'png', compress_level=1)

    sio.seek(0)
    return base64.b64encode(sio.getvalue()).decode()
