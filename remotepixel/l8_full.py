"""remotepixel.l8_full"""

import re
import contextlib
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

import rasterio
from rasterio.io import MemoryFile
from rio_toa import reflectance

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def create(scene, bands=None, expression=None):
    """
    """
    scene_params = utils.landsat_parse_scene_id(scene)
    meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    if not expression and not bands:
        raise Exception('Expression or Bands must be provided')

    if bands:
        nb_bands = len(bands)
        data_type = np.uint16
        if nb_bands != 3:
            raise Exception('RGB combination only')

    if expression:
        bands = list(set(re.findall(r'b(?P<bands>[0-9]{1,2})', expression)))
        rgb = expression.split(',')
        data_type = np.float32
        nb_bands = len(rgb)

    bqa = f'{landsat_address}_BQA.TIF'
    with rasterio.open(bqa) as src:
        meta = src.meta
        wind = [w for ij, w in src.block_windows(1)]

    meta.update(nodata=0, count=nb_bands, interleave='pixel', compress='LZW',
                photometric='MINISBLACK' if expression else 'RGB', dtype=data_type)

    sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

    memfile = MemoryFile()
    with memfile.open(**meta) as dataset:
        with contextlib.ExitStack() as stack:
            srcs = [stack.enter_context(rasterio.open(f'{landsat_address}_B{band}.TIF')) for band in bands]

            def get_window(idx, window):
                band = bands[idx]
                multi_reflect = meta_data['RADIOMETRIC_RESCALING'].get(f'REFLECTANCE_MULT_BAND_{band}')
                add_reflect = meta_data['RADIOMETRIC_RESCALING'].get(f'REFLECTANCE_ADD_BAND_{band}')
                data = srcs[idx].read(window=window, boundless=True, indexes=(1))
                return reflectance.reflectance(data, multi_reflect, add_reflect, sun_elev)

            for window in wind:
                _worker = partial(get_window, window=window)
                with futures.ThreadPoolExecutor(max_workers=3) as executor:
                    data = np.stack(list(executor.map(_worker, range(len(bands)))))
                    if expression:
                        ctx = {}
                        for bdx, b in enumerate(bands):
                            ctx['b{}'.format(b)] = data[bdx]
                        data = np.array([np.nan_to_num(ne.evaluate(bloc.strip(), local_dict=ctx)) for bloc in rgb])
                    else:
                        data *= 10000

                    dataset.write(data.astype(data_type), window=window)

    return memfile
