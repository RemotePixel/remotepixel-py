
import os

import pytest

from remotepixel import cbers_ovr

CBERS_SCENE = "CBERS_4_MUX_20171121_057_094_L2"
CBERS_BUCKET = os.path.join(os.path.dirname(__file__), "fixtures", "cbers-pds")
CBERS_PATH = os.path.join(
    CBERS_BUCKET, "CBERS4/MUX/057/094/CBERS_4_MUX_20171121_057_094_L2/"
)
CBERS_ADRESS = f"{CBERS_PATH}{CBERS_SCENE}_BAND6.tif"


def test_create_valid(monkeypatch):
    """Should work as expected (read r,g,b bands and create JPEG image)."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    assert cbers_ovr.create(CBERS_SCENE, bands=[7, 6, 5])


def test_create_validPNG(monkeypatch):
    """Should work as expected (read r,g,b bands and create PNG image)."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    assert cbers_ovr.create(CBERS_SCENE, bands=[7, 6, 5], img_format="png")


def test_create_validSmall(monkeypatch):
    """Should work as expected (read bands and create 128x128 image)."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    assert cbers_ovr.create(CBERS_SCENE, bands=[7, 6, 5], ovrSize=128)


def test_create_invalidFormat(monkeypatch):
    """Should raise invalid format."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    with pytest.raises(UserWarning):
        cbers_ovr.create(CBERS_SCENE, bands=[7, 6, 5], img_format="tif")


def test_create_lessband(monkeypatch):
    """Should raise on not many bands."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    with pytest.raises(Exception):
        cbers_ovr.create(CBERS_SCENE, bands=[7, 6])


def test_create_npbandexpress(monkeypatch):
    """Should raise."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    with pytest.raises(Exception):
        cbers_ovr.create(CBERS_SCENE)


def test_create_validexpression(monkeypatch):
    """Should work as expected (read r,g,b bands and create JPEG image)."""
    monkeypatch.setattr(cbers_ovr, "CBERS_BUCKET", CBERS_BUCKET)
    expression = "(b8 - b7) / (b8 + b7)"
    assert cbers_ovr.create(CBERS_SCENE, expression=expression)
