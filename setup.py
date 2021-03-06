#!/usr/bin/env python3

from distutils.core import setup
from pkg_resources import parse_requirements
from setuptools import find_packages

with open("requirements.txt") as f:
    REQUIREMENTS = [str(req) for req in parse_requirements(f.read())]

with open("requirements-dev.txt") as f:
    REQUIREMENTS_DEV = [str(req) for req in parse_requirements(f.read())]

with open("version.txt") as f:
    SEMANTIC_VERSION = str(f.read())

setup(
    name="todb",
    version=SEMANTIC_VERSION,
    description="experimental project: importing csv data into any SQL DB in a smart way",
    author="Mateusz Korzeniowski",
    author_email="emkor93@gmail.com",
    url="https://github.com/emkor/todb",
    packages=find_packages(exclude=("test", "test.*")),
    install_requires=REQUIREMENTS,
    tests_require=REQUIREMENTS_DEV,
    entry_points={
        'console_scripts': [
            'todb = todb.main:cli_main'
        ]
    }
)
