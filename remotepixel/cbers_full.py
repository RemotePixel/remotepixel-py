"""remotepixel.l8_full"""

import re
import contextlib
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

import rasterio
from rasterio.io import MemoryFile

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

CBERS_BUCKET = 's3://cbers-pds'


def create(scene, bands=None, expression=None):
    """
    """

    scene_params = utils.cbers_parse_scene_id(scene)
    cbers_address = f'{CBERS_BUCKET}/{scene_params["key"]}'

    if not expression and not bands:
        raise Exception('Expression or Bands must be provided')

    if bands:
        nb_bands = len(bands)
        data_type = np.uint8
        if nb_bands != 3:
            raise Exception('RGB combination only')

    if expression:
        bands = tuple(set(re.findall(r'b(?P<bands>[0-9]{1,2})', expression)))
        rgb = expression.split(',')
        data_type = np.float32
        nb_bands = len(rgb)

    bqa = f'{cbers_address}/{scene}_BAND6.tif'
    with rasterio.open(bqa) as src:
        meta = src.meta
        wind = [w for ij, w in src.block_windows(1)]

        meta.update(nodata=0, count=nb_bands, interleave='pixel', compress='LZW',
                    photometric='MINISBLACK' if expression else 'RGB', dtype=data_type)

    memfile = MemoryFile()
    with memfile.open(**meta) as dataset:
        with contextlib.ExitStack() as stack:
            srcs = [stack.enter_context(rasterio.open(f'{cbers_address}/{scene}_BAND{band}.tif')) for band in bands]

            def get_window(idx, window):
                return srcs[idx].read(window=window, boundless=True, indexes=(1))

            for window in wind:
                _worker = partial(get_window, window=window)
                with futures.ThreadPoolExecutor(max_workers=3) as executor:
                    data = np.stack(list(executor.map(_worker, range(len(bands)))))
                    if expression:
                        ctx = {}
                        for bdx, b in enumerate(bands):
                            ctx['b{}'.format(b)] = data[bdx]
                        data = np.array([np.nan_to_num(ne.evaluate(bloc.strip(), local_dict=ctx)) for bloc in rgb])

                    dataset.write(data.astype(data_type), window=window)

    return memfile
