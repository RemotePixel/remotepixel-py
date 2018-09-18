"""remotepixel.s2_ndvi module."""

import os
import re
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

import rasterio
from rasterio import warp

from remotepixel.utils import sentinel2_get_info, get_area
from rio_tiler.utils import (
    sentinel_parse_scene_id,
    linear_rescale,
    array_to_img,
    get_colormap,
    b64_encode_img,
)


np.seterr(divide="ignore", invalid="ignore")

SENTINEL_BUCKET = "s3://sentinel-s2-l1c"


def point(scene, coordinates, expression):
    """Point handler."""
    bands = tuple(set(re.findall(r"b(?P<bands>[0-9]{1,2})", expression)))

    scene_params = sentinel_parse_scene_id(scene)
    sentinel_address = f'{SENTINEL_BUCKET}/{scene_params["key"]}'
    scene_info = sentinel2_get_info(
        os.path.basename(SENTINEL_BUCKET), scene_params["key"], request_pays=True
    )

    addresses = [f"{sentinel_address}/B{band}.jp2" for band in bands]

    def worker(address):
        """Worker."""
        with rasterio.open(address) as band:
            lon_srs, lat_srs = warp.transform(
                "EPSG:4326", band.crs, [coordinates[0]], [coordinates[1]]
            )
            point = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]
        return point[0]

    try:
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            data = list(executor.map(worker, addresses))

            ctx = {}
            for bdx, b in enumerate(bands):
                ctx["b{}".format(b)] = data[bdx]
            ratio = np.nan_to_num(ne.evaluate(expression, local_dict=ctx))
    except IndexError:
        ratio = 0.

    date = (
        scene_params["acquisitionYear"]
        + "-"
        + scene_params["acquisitionMonth"]
        + "-"
        + scene_params["acquisitionDay"]
    )

    return {
        "ndvi": ratio,
        "date": date,
        "sat": scene_info["sat"],
        "scene": scene,
        "cloud": scene_info["cloud_coverage"],
    }


def area(
    scene,
    bbox,
    expression,
    expression_range=[-1, 1],
    bbox_crs="epsg:4326",
    out_crs="epsg:3857",
):
    """Area handler."""
    max_img_size = 512

    bands = tuple(set(re.findall(r"b(?P<bands>[0-9]{1,2})", expression)))

    scene_params = sentinel_parse_scene_id(scene)
    sentinel_address = f'{SENTINEL_BUCKET}/{scene_params["key"]}'
    scene_info = sentinel2_get_info(
        os.path.basename(SENTINEL_BUCKET), scene_params["key"], request_pays=True
    )

    addresses = [f"{sentinel_address}/B{band}.jp2" for band in bands]

    _worker = partial(
        get_area,
        bbox=bbox,
        max_img_size=max_img_size,
        bbox_crs=bbox_crs,
        out_crs=out_crs,
    )
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        data = np.concatenate(list(executor.map(_worker, addresses)))
        if not np.any(data):
            raise Exception("No valid data in array")
        mask = np.all(data != 0, axis=0).astype(np.uint8) * 255

        ctx = {}
        for bdx, b in enumerate(bands):
            ctx["b{}".format(b)] = data[bdx]
        ratio = np.nan_to_num(ne.evaluate(expression, local_dict=ctx))

    ratio = np.where(
        mask, linear_rescale(ratio, in_range=expression_range, out_range=[0, 255]), 0
    ).astype(np.uint8)

    img = array_to_img(ratio, mask, get_colormap(name="cfastie"))
    ndvi = b64_encode_img(img, "jpeg")

    date = (
        scene_params["acquisitionYear"]
        + "-"
        + scene_params["acquisitionMonth"]
        + "-"
        + scene_params["acquisitionDay"]
    )

    return {
        "ndvi": ndvi,
        "date": date,
        "sat": scene_info["sat"],
        "scene": scene,
        "cloud": scene_info["cloud_coverage"],
    }
