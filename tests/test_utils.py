"""Test remotepixel.utils ."""

import os

import pytest

from rio_toa import toa_utils

from remotepixel import utils


mtl_file = os.path.join(
    os.path.dirname(__file__), "fixtures", "LC80140352017115LGN00_MTL.txt"
)
with open(mtl_file, "r") as f:
    meta_data = toa_utils._parse_mtl_txt(f.read())

with open(mtl_file, "r") as f:
    meta_data_raw = f.read().encode("utf-8")


def test_get_overview_validLandsat():
    """Valid Landsat."""
    landsat_scene_c1 = "LC08_L1TP_016037_20170813_20170814_01_RT"
    landsat_bucket = os.path.join(os.path.dirname(__file__), "fixtures", "landsat-pds")
    landsat_path = os.path.join(
        landsat_bucket, "c1", "L8", "016", "037", landsat_scene_c1, landsat_scene_c1
    )
    address = f"{landsat_path}_B4.TIF"
    assert utils.get_overview(address, 512).shape == (1, 512, 512)


def test_get_overview_validSentinel():
    """Valid Sentinel."""
    sentinel_bucket = os.path.join(
        os.path.dirname(__file__), "fixtures", "sentinel-s2-l1c"
    )
    sentinel_path = os.path.join(sentinel_bucket, "tiles/19/U/DP/2017/7/29/0/")
    address = f"{sentinel_path}/B04.jp2"
    assert utils.get_overview(address, 512).shape == (1, 512, 512)


def test_cbers_id_invalid():
    """Should raise an error with invalid sceneid."""
    scene = "CBERS_4_MUX_20171121_057_094"
    with pytest.raises(ValueError):
        utils.cbers_parse_scene_id(scene)


def test_cbers_id_valid():
    """Should work as expected (parse cbers scene id)."""
    scene = "CBERS_4_MUX_20171121_057_094_L2"
    expected_content = {
        "acquisitionDay": "21",
        "acquisitionMonth": "11",
        "acquisitionYear": "2017",
        "intrument": "MUX",
        "key": "CBERS4/MUX/057/094/CBERS_4_MUX_20171121_057_094_L2",
        "path": "057",
        "processingCorrectionLevel": "L2",
        "row": "094",
        "satellite": "4",
        "scene": "CBERS_4_MUX_20171121_057_094_L2",
        "sensor": "CBERS",
    }
    assert utils.cbers_parse_scene_id(scene) == expected_content
