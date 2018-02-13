"""remotepixel.l8_full"""

import re
import uuid

import boto3
import numpy as np
import numexpr as ne

import rasterio
from rasterio.io import MemoryFile

from remotepixel import utils

np.seterr(divide='ignore', invalid='ignore')

CBERS_BUCKET = 's3://cbers-pds'


def create(scene, out_bucket, bands=None, expression=None, output_uid=None):
    """
    """
    if not output_uid:
        output_uid = str(uuid.uuid1())

    scene_params = utils.cbers_parse_scene_id(scene)
    cbers_address = f'{CBERS_BUCKET}/{scene_params["key"]}'

    if not expression and not bands:
        raise Exception('Expression or Bands must be provided')

    if bands:
        nb_bands = len(bands)
        data_type = np.uint16
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

        meta.update(nodata=0,
                    count=nb_bands,
                    interleave='pixel',
                    PHOTOMETRIC='MINISBLACK' if expression else 'RGB',
                    dtype=data_type)

    def get_window(band, window):
        out_shape = (1, window.height, window.width)
        band_address = f'{cbers_address}/{scene}_BAND{band}.tif'
        with rasterio.open(band_address) as src:
            return src.read(window=window, boundless=True, out_shape=out_shape, indexes=(1))

    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:
            for window in wind:
                data = [get_window(band, window) for band in bands]
                data = np.stack(data)
                if expression:
                    ctx = {}
                    for bdx, b in enumerate(bands):
                        ctx['b{}'.format(b)] = data[bdx]
                    data = np.array([np.nan_to_num(ne.evaluate(bloc.strip(), local_dict=ctx)) for bloc in rgb])
                else:
                    data *= 10000

                dataset.write(data.astype(data_type), window=window)

        params = {
            'ACL': 'public-read',
            'Metadata': {
                'scene': 'scene'},
            'ContentType': 'image/tiff'}

        if expression:
            params['Metadata']['expression'] = expression
        else:
            params['Metadata']['bands'] = ''.join(map(str, bands))

        key = f'data/cbers/{output_uid}.tif'
        client = boto3.client('s3')
        client.upload_fileobj(memfile, out_bucket, key, ExtraArgs=params)

    return True
