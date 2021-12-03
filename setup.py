#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-rest",
    version="0.1.0",
    description="Singer.io target for extracting data to REST API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_rest"],
    install_requires=[
        "singer-python==5.12.2",
        "requests==2.25.1"
    ],
    entry_points="""
    [console_scripts]
    target-rest=target_rest:main
    """,
    packages=["target_rest"],
    package_data = {},
    include_package_data=True,
)
