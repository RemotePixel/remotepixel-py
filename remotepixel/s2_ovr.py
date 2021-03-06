"""remotepixel.s2_ovr module."""

import re
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

from remotepixel.utils import get_overview
from rio_tiler.utils import (
    sentinel_parse_scene_id,
    linear_rescale,
    array_to_img,
    get_colormap,
    b64_encode_img,
)

np.seterr(divide="ignore", invalid="ignore")

SENTINEL_BUCKET = "s3://sentinel-s2-l1c"


def worker(band, sentinel_address, ovr_size):
    """Worker."""
    address = f"{sentinel_address}/B{band}.jp2"
    return get_overview(address, ovr_size)


def create(
    scene,
    bands=None,
    expression=None,
    expression_range=[-1, 1],
    img_format="jpeg",
    ovrSize=512,
):
    """Handler."""
    if img_format not in ["png", "jpeg"]:
        raise UserWarning(f"Invalid {img_format} extension")

    if not expression and not bands:
        raise Exception("Expression or Bands must be provided")

    if bands:
        nb_bands = len(bands)
        if nb_bands != 3:
            raise Exception("RGB combination only")

    if expression:
        bands = tuple(set(re.findall(r"b(?P<bands>[0-9A]{1,2})", expression)))
        rgb = expression.split(",")
        nb_bands = len(rgb)

    scene_params = sentinel_parse_scene_id(scene)
    sentinel_address = f'{SENTINEL_BUCKET}/{scene_params["key"]}'

    _worker = partial(worker, sentinel_address=sentinel_address, ovr_size=ovrSize)
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        data = np.concatenate(list(executor.map(_worker, bands)))
        mask = np.all(data != 0, axis=0).astype(np.uint8) * 255

        if expression:
            ctx = {}
            for bdx, b in enumerate(bands):
                ctx["b{}".format(b)] = data[bdx]
            data = np.array(
                [
                    np.nan_to_num(ne.evaluate(bloc.strip(), local_dict=ctx))
                    for bloc in rgb
                ]
            )

        for band in range(data.shape[0]):
            imgRange = (
                expression_range
                if expression
                else np.percentile(data[band][mask > 0], (2, 98)).tolist()
            )
            data[band] = np.where(
                mask,
                linear_rescale(data[band], in_range=imgRange, out_range=[0, 255]),
                0,
            )

    data = data.squeeze()
    colormap = None if len(data.shape) >= 3 else get_colormap(name="cfastie")

    img = array_to_img(data, mask, colormap)
    return b64_encode_img(img, img_format)
