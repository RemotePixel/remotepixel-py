
from setuptools import setup, find_packages

with open("remotepixel/__init__.py") as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break

# Runtime requirements.
inst_reqs = [
    "numpy",
    "Pillow",
    "mercantile",
    "rasterio[s3]~=1.0",
    "rio-tiler>=1.0rc2",
    "rio-toa",
    "numexpr",
]

extra_reqs = {"test": ["pytest", "pytest-cov", "mock"]}


setup(
    name="remotepixel",
    version=version,
    description=u"",
    long_description=u"",
    classifiers=["Programming Language :: Python :: 3.6"],
    keywords="remotepixel AWS lambda Landsat Sentinel SRTM",
    author=u"RemotePixel",
    author_email="contact@remotepixel.ca",
    url="https://github.com/remotepixel/remotepixel-py",
    license="BSD-2",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    python_requires="~=3.6",
    package_data={"remotepixel": ["data/cmap.txt"]},
    install_requires=inst_reqs,
    extras_require=extra_reqs,
)
