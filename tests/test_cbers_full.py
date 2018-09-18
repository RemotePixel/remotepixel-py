
import os
import pytest

from remotepixel import cbers_full

CBERS_SCENE = "CBERS_4_MUX_20171121_057_094_L2"
CBERS_BUCKET = os.path.join(os.path.dirname(__file__), "fixtures", "cbers-pds")


def test_create_bands(monkeypatch):
    """Should work as expected (read data, create RGB)."""
    monkeypatch.setattr(cbers_full, "CBERS_BUCKET", CBERS_BUCKET)
    bands = [7, 6, 5]
    assert cbers_full.create(CBERS_SCENE, bands=bands)


def test_create_expression(monkeypatch):
    """Should work as expected (read data, create NDVI)."""
    expression = "(b8 - b7) / (b8 + b7)"
    monkeypatch.setattr(cbers_full, "CBERS_BUCKET", CBERS_BUCKET)
    assert cbers_full.create(CBERS_SCENE, expression=expression)


def test_create_lessBand(monkeypatch):
    """Should raise an error."""
    monkeypatch.setattr(cbers_full, "CBERS_BUCKET", CBERS_BUCKET)
    bands = [7, 6]
    with pytest.raises(Exception):
        cbers_full.create(CBERS_SCENE, bands=bands)


def test_create_noExpreBands(monkeypatch):
    """Shoult raise an error on missing expression or band indexes."""
    monkeypatch.setattr(cbers_full, "CBERS_BUCKET", CBERS_BUCKET)
    with pytest.raises(Exception):
        cbers_full.create(CBERS_SCENE)
