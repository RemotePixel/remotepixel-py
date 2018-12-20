"""remotepixel utils."""

import re
import json

import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from rasterio.warp import transform_bounds

from remotepixel import aws
from rio_tiler.utils import get_vrt_transform


def get_area(
    address, bbox, max_img_size=512, bbox_crs="epsg:4326", out_crs="epsg:3857", nodata=0
):
    """Read image part."""
    bounds = transform_bounds(bbox_crs, out_crs, *bbox, densify_pts=21)

    vrt_params = dict(add_alpha=True, crs=out_crs, resampling=Resampling.bilinear)

    if nodata is not None:
        vrt_params.update(
            dict(
                nodata=nodata,
                add_alpha=False,
                src_nodata=nodata,
                init_dest_nodata=False,
            )
        )

    with rasterio.open(address) as src:
        vrt_transform, vrt_width, vrt_height = get_vrt_transform(src, bounds)

        vrt_width = round(vrt_width) if vrt_width < max_img_size else max_img_size
        vrt_height = round(vrt_height) if vrt_height < max_img_size else max_img_size
        vrt_params.update(
            dict(transform=vrt_transform, width=vrt_width, height=vrt_height)
        )

        with WarpedVRT(src, **vrt_params) as vrt:
            data = vrt.read(
                out_shape=(1, vrt_height, vrt_width),
                resampling=Resampling.bilinear,
                indexes=[1],
            )

    return data


def get_overview(address, ovrSize):
    """Get Overview."""
    with rasterio.open(address) as src:
        matrix = src.read(
            indexes=[1], out_shape=(1, ovrSize, ovrSize), resampling=Resampling.bilinear
        )
    return matrix


def zeroPad(n, l):
    """Add leading 0."""
    return str(n).zfill(l)


def sentinel2_get_info(bucket, scene_path, request_pays=False):
    """Get sentinel-2 metadata."""
    data = json.loads(
        aws.get_object(bucket, f"{scene_path}/tileInfo.json", request_pays=request_pays)
    )
    return {
        "sat": data["productName"][0:3],
        "coverage": data.get("dataCoveragePercentage"),
        "cloud_coverage": data.get("cloudyPixelPercentage"),
    }


def cbers_parse_scene_id(sceneid):
    """Parse CBERS scene id."""
    if not re.match("^CBERS_4_MUX_[0-9]{8}_[0-9]{3}_[0-9]{3}_L[0-9]$", sceneid):
        raise ValueError("Could not match {}".format(sceneid))

    cbers_pattern = (
        r"(?P<sensor>\w{5})"
        r"_"
        r"(?P<satellite>[0-9]{1})"
        r"_"
        r"(?P<intrument>\w{3})"
        r"_"
        r"(?P<acquisitionYear>[0-9]{4})"
        r"(?P<acquisitionMonth>[0-9]{2})"
        r"(?P<acquisitionDay>[0-9]{2})"
        r"_"
        r"(?P<path>[0-9]{3})"
        r"_"
        r"(?P<row>[0-9]{3})"
        r"_"
        r"(?P<processingCorrectionLevel>L[0-9]{1})$"
    )

    meta = None
    match = re.match(cbers_pattern, sceneid, re.IGNORECASE)
    if match:
        meta = match.groupdict()

    path = meta["path"]
    row = meta["row"]
    meta["key"] = "CBERS4/MUX/{}/{}/{}".format(path, row, sceneid)

    meta["scene"] = sceneid

    return meta
