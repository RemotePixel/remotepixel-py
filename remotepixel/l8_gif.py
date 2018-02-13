"""remotepixel.l8_gif"""

from io import BytesIO
from functools import partial
from concurrent import futures

import numpy as np
from PIL import Image, ImageFont, ImageDraw
from wand.image import Image as WImage

from rasterio.plot import reshape_as_image
from rio_toa import reflectance

from remotepixel import utils

LANDSAT_BUCKET = 's3://landsat-pds'
font = ImageFont.load_default().font


def worker(scene, bbox=None, rgb=[4, 3, 2], img_size=512):

    try:
        scene_params = utils.landsat_parse_scene_id(scene)
        meta_data = utils.landsat_get_mtl(scene).get('L1_METADATA_FILE')
        landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

        sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

        if not bbox:
            out = np.stack([utils.get_overview(f'{landsat_address}_B{b}.TIF', img_size) for b in rgb])
        else:
            out = np.stack([utils.get_area(f'{landsat_address}_B{b}.TIF', bbox, img_size) for b in rgb])

        for bdx, band in enumerate(rgb):
            multi_reflect = meta_data['RADIOMETRIC_RESCALING'].get(
                f'REFLECTANCE_MULT_BAND_{band}')

            add_reflect = meta_data['RADIOMETRIC_RESCALING'].get(
                f'REFLECTANCE_ADD_BAND_{band}')

            out[bdx] = 10000 * reflectance.reflectance(
                out[bdx], multi_reflect, add_reflect, sun_elev)

            minRef = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MINIMUM_BAND_{band}'] * 10000
            maxRef = meta_data['MIN_MAX_REFLECTANCE'][f'REFLECTANCE_MAXIMUM_BAND_{band}'] * 10000

            out[bdx] = np.where(out[bdx] > 0,
                                utils.linear_rescale(out[bdx], in_range=[minRef, maxRef], out_range=[1, 255]),
                                0)

        img = Image.fromarray(reshape_as_image(out.astype(np.uint8)))
        draw = ImageDraw.Draw(img)
        draw.text((10, 10), scene_params['date'], (255, 255, 255), font=font)

        sio = BytesIO()
        img.save(sio, 'jpeg', subsampling=0, quality=100)
        sio.seek(0)
        img = None

        return sio

    except:
        return None


def create(scenes, bbox=None, rgb=[4, 3, 2]):
    """
    """

    _worker = partial(worker, bbox=bbox, rgb=rgb)
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        allimg = list(executor.map(_worker, scenes))

    with WImage() as gif:
        for sio in allimg:
            if not sio:
                continue
            with WImage(file=sio, format='jpeg') as img:
                gif.sequence.append(img)

        # Create progressive delay for each frame
        for cursor in range(len(allimg)):
            with gif.sequence[cursor] as frame:
                frame.delay = 1000 * (cursor + 1)

        gif.type = 'optimize'
        sio = BytesIO()
        gif.save(file=sio)
        sio.seek(0)

        return sio

        # meta = {
        #     'id': uuid,
        #     'mosaic': '{}.gif'.format(uuid),
        #     'coordinates': {
        #         'north': bbox[3],
        #         'west': bbox[0],
        #         'south': bbox[1],
        #         'east': bbox[2],
        #         'Proj': 'EPSG:4326'}}
        #
        # client = boto3.client('s3')
        # key = f'data/mosaic/{uuid}.gif'
        # client.upload_fileobj(sio.getvalue(), bucket, key,
        #                       ExtraArgs={
        #                             'ACL': 'public-read',
        #                             'ContentType': 'image/gif'})
        #
        # client.put_object(
        #     ACL='public-read',
        #     Bucket=bucket,
        #     Key=f'data/mosaic/{uuid}.json',
        #     Body=json.dumps(meta),
        #     ContentType='application/json')
