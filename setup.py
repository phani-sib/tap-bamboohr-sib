#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bamboohr-sib",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bamboohr_sib"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-bamboohr-sib=tap_bamboohr_sib:main
    """,
    packages=["tap_bamboohr_sib"],
    package_data = {
        "schemas": ["tap_bamboohr_sib/schemas/*.json"]
    },
    include_package_data=True,
)
