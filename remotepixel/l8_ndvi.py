"""remotepixel.l8_ndvi module."""

import re
from functools import partial
from concurrent import futures

import numpy as np
import numexpr as ne

import rasterio
from rasterio import warp
from rio_toa.reflectance import reflectance

from remotepixel.utils import get_area
from rio_tiler.utils import (
    landsat_parse_scene_id,
    landsat_get_mtl,
    linear_rescale,
    array_to_img,
    get_colormap,
    b64_encode_img,
)


np.seterr(divide="ignore", invalid="ignore")

LANDSAT_BUCKET = "s3://landsat-pds"


def point(scene, coordinates, expression):
    """Point handler."""
    bands = tuple(set(re.findall(r"b(?P<bands>[0-9]{1,2})", expression)))

    scene_params = landsat_parse_scene_id(scene)
    meta_data = landsat_get_mtl(scene).get("L1_METADATA_FILE")
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    def worker(band, coordinates):
        """Worker."""
        address = f"{landsat_address}_B{band}.TIF"
        sun_elev = meta_data["IMAGE_ATTRIBUTES"]["SUN_ELEVATION"]
        multi_reflect = meta_data["RADIOMETRIC_RESCALING"][
            f"REFLECTANCE_MULT_BAND_{band}"
        ]
        add_reflect = meta_data["RADIOMETRIC_RESCALING"][f"REFLECTANCE_ADD_BAND_{band}"]
        with rasterio.open(address) as band:
            lon_srs, lat_srs = warp.transform(
                "EPSG:4326", band.crs, [coordinates[0]], [coordinates[1]]
            )
            point = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]

        return reflectance(point, multi_reflect, add_reflect, sun_elev, src_nodata=0)[0]

    try:
        _worker = partial(worker, coordinates=coordinates)
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            data = list(executor.map(_worker, bands))

            ctx = {}
            for bdx, b in enumerate(bands):
                ctx["b{}".format(b)] = data[bdx]
            ratio = np.nan_to_num(ne.evaluate(expression, local_dict=ctx))
    except IndexError:
        ratio = 0.0

    return {
        "ndvi": ratio,
        "date": scene_params["date"],
        "scene": scene,
        "cloud": meta_data["IMAGE_ATTRIBUTES"]["CLOUD_COVER"],
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

    scene_params = landsat_parse_scene_id(scene)
    meta_data = landsat_get_mtl(scene).get("L1_METADATA_FILE")
    landsat_address = f'{LANDSAT_BUCKET}/{scene_params["key"]}'

    def worker(band):
        """Worker."""
        address = f"{landsat_address}_B{band}.TIF"
        sun_elev = meta_data["IMAGE_ATTRIBUTES"]["SUN_ELEVATION"]
        multi_reflect = meta_data["RADIOMETRIC_RESCALING"][
            f"REFLECTANCE_MULT_BAND_{band}"
        ]
        add_reflect = meta_data["RADIOMETRIC_RESCALING"][f"REFLECTANCE_ADD_BAND_{band}"]

        band = get_area(
            address, bbox, max_img_size=max_img_size, bbox_crs=bbox_crs, out_crs=out_crs
        )
        return reflectance(band, multi_reflect, add_reflect, sun_elev, src_nodata=0)

    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        data = np.concatenate(list(executor.map(worker, bands)))
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

    return {
        "ndvi": ndvi,
        "date": scene_params["date"],
        "scene": scene,
        "cloud": meta_data["IMAGE_ATTRIBUTES"]["CLOUD_COVER"],
    }
