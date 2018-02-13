import os
import re
import datetime

from urllib.request import urlopen

import numpy as np

import rasterio
from rasterio.enums import Resampling
from rasterio import warp

from rio_toa import toa_utils


def get_colormap():
    """
    """
    cmap_file = os.path.join(os.path.dirname(__file__), 'cmap.txt')
    with open(cmap_file) as cmap:
        lines = cmap.read().splitlines()
        colormap = [list(map(int, line.split())) for line in lines if not line.startswith('#')][1:]

    return colormap


def get_area(address, bbox, img_size):
    """
    """
    with rasterio.open(address) as src:
        crs_bounds = warp.transform_bounds('EPSG:4326', src.crs, *bbox)
        window = src.window(*crs_bounds)
        width = round(window.width) if window.width < img_size else img_size
        height = round(window.height) if window.height < img_size else img_size

        matrix = src.read(window=window,
                          out_shape=(1, height, width),
                          indexes=1,
                          resampling=Resampling.bilinear)

        return matrix


def get_overview(address, ovrSize):
    """
    """

    out_shape = (1, ovrSize, ovrSize)
    with rasterio.open(address) as src:
        data = src.read(indexes=(1),
                        out_shape=out_shape,
                        resampling=Resampling.bilinear)

        return np.expand_dims(data, axis=0)


def linear_rescale(image, in_range=[0, 16000], out_range=[1, 255]):
    """
    Linear rescaling
    """

    imin, imax = in_range
    omin, omax = out_range
    image = np.clip(image, imin, imax) - imin
    image = image / float(imax - imin)

    return (image * (omax - omin) + omin)


def landsat_get_mtl(sceneid):
    """Get Landsat-8 MTL metadata

    Attributes
    ----------

    sceneid : str
        Landsat sceneid. For scenes after May 2017,
        sceneid have to be LANDSAT_PRODUCT_ID.

    Returns
    -------
    out : dict
        returns a JSON like object with the metadata.
    """

    try:
        scene_params = landsat_parse_scene_id(sceneid)
        meta_file = 'http://landsat-pds.s3.amazonaws.com/{}_MTL.txt'.format(scene_params['key'])
        metadata = str(urlopen(meta_file).read().decode())
        return toa_utils._parse_mtl_txt(metadata)
    except:
        raise Exception('Could not retrieve {} metadata'.format(sceneid))


def landsat_parse_scene_id(sceneid):
    """
    Author @perrygeo - http://www.perrygeo.com

    parse scene id
    """

    if not re.match('^(L[COTEM]8\d{6}\d{7}[A-Z]{3}\d{2})|(L[COTEM]08_L\d{1}[A-Z]{2}_\d{6}_\d{8}_\d{8}_\d{2}_(T1|T2|RT))$', sceneid):
        raise ValueError(f'Could not match {sceneid}')

    precollection_pattern = (
        r'^L'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>\w{1})'
        r'(?P<path>[0-9]{3})'
        r'(?P<row>[0-9]{3})'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionJulianDay>[0-9]{3})'
        r'(?P<groundStationIdentifier>\w{3})'
        r'(?P<archiveVersion>[0-9]{2})$')

    collection_pattern = (
        r'^L'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>\w{2})'
        r'_'
        r'(?P<processingCorrectionLevel>\w{4})'
        r'_'
        r'(?P<path>[0-9]{3})'
        r'(?P<row>[0-9]{3})'
        r'_'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionMonth>[0-9]{2})'
        r'(?P<acquisitionDay>[0-9]{2})'
        r'_'
        r'(?P<processingYear>[0-9]{4})'
        r'(?P<processingMonth>[0-9]{2})'
        r'(?P<processingDay>[0-9]{2})'
        r'_'
        r'(?P<collectionNumber>\w{2})'
        r'_'
        r'(?P<collectionCategory>\w{2})$')

    meta = None
    for pattern in [collection_pattern, precollection_pattern]:
        match = re.match(pattern, sceneid, re.IGNORECASE)
        if match:
            meta = match.groupdict()
            break

    if not meta:
        raise ValueError(f'Could not match {sceneid}')

    if meta.get('acquisitionJulianDay'):
        date = datetime.datetime(int(meta['acquisitionYear']), 1, 1) \
            + datetime.timedelta(int(meta['acquisitionJulianDay']) - 1)

        meta['date'] = date.strftime('%Y-%m-%d')
    else:
        meta['date'] = f'{meta.get("acquisitionYear")}-{meta.get("acquisitionMonth")}-{meta.get("acquisitionDay")}'

    collection = meta.get('collectionNumber', '')
    if collection != '':
        collection = f'c{int(collection)}'

    meta['key'] = os.path.join(collection, 'L8', meta['path'], meta['row'], sceneid, sceneid)
    meta['scene'] = sceneid

    return meta


def sentinel_parse_scene_id(sceneid):
    """
    parse scene id
    """

    if not re.match('^S2[AB]_tile_[0-9]{8}_[0-9]{2}[A-Z]{3}_[0-9]$', sceneid):
        raise ValueError(f'Could not match {sceneid}')

    sentinel_pattern = (
        r'^S'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>[AB]{1})'
        r'_tile_'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionMonth>[0-9]{2})'
        r'(?P<acquisitionDay>[0-9]{2})'
        r'_'
        r'(?P<utm>[0-9]{2})'
        r'(?P<sq>\w{1})'
        r'(?P<lat>\w{2})'
        r'_'
        r'(?P<num>[0-9]{1})$')

    meta = None
    match = re.match(sentinel_pattern, sceneid, re.IGNORECASE)
    if match:
        meta = match.groupdict()

    if not meta:
        raise ValueError('Could not match {sceneid}')

    utm = meta['utm']
    sq = meta['sq']
    lat = meta['lat']
    year = meta['acquisitionYear']
    m = meta['acquisitionMonth'].lstrip("0")
    d = meta['acquisitionDay'].lstrip("0")
    n = meta['num']

    meta['key'] = f'tiles/{utm}/{sq}/{lat}/{year}/{m}/{d}/{n}'

    return meta


def cbers_parse_scene_id(sceneid):
    """Parse CBERS scene id"""

    if not re.match('^CBERS_4_MUX_[0-9]{8}_[0-9]{3}_[0-9]{3}_L[0-9]$', sceneid):
        raise ValueError('Could not match {}'.format(sceneid))

    cbers_pattern = (
        r'(?P<sensor>\w{5})'
        r'_'
        r'(?P<satellite>[0-9]{1})'
        r'_'
        r'(?P<intrument>\w{3})'
        r'_'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionMonth>[0-9]{2})'
        r'(?P<acquisitionDay>[0-9]{2})'
        r'_'
        r'(?P<path>[0-9]{3})'
        r'_'
        r'(?P<row>[0-9]{3})'
        r'_'
        r'(?P<processingCorrectionLevel>L[0-9]{1})$')

    meta = None
    match = re.match(cbers_pattern, sceneid, re.IGNORECASE)
    if match:
        meta = match.groupdict()

    path = meta['path']
    row = meta['row']
    meta['key'] = 'CBERS4/MUX/{}/{}/{}'.format(path, row, sceneid)

    meta['scene'] = sceneid

    return meta
