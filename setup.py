#!/usr/bin/env python3

import os
import setuptools


here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.rst')) as f:
        README = f.read()
except IOError:
    README = ''

setuptools.setup(
    name='bracon',
    version='0.0.1',
    description="Branch and Confluence. You can have consecutive another process.",
    long_description=README,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Libraries"
    ],
    keywords=['reactive'],
    author='Motoki Naruse',
    author_email='motoki@naru.se',
    url="https://github.com/narusemotoki/bracon",
    license='MIT',
    packages=setuptools.find_packages(),
    zip_safe=False,
)
