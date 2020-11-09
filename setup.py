"""
Created on Mar 18, 2015


@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""
from setuptools import setup

setup(
    name="calcbench_api_client",
    version="3.0.9",
    description="Client for Calcbench data.",
    author="Andrew Kittredge",
    author_email="andrew@calcbench.com",
    license="Apache2",
    keywords="finance accounting SEC 10-(K|Q)",
    install_requires=[
        "requests",
        "typing_extensions; python_version < '3.7'",
        "dataclasses; python_version < '3.7'",
    ],
    python_requires=">=3.6",
    packages=["calcbench"],
    extras_require={
        "Pandas": ["Pandas"],
        "Listener": ["azure-servicebus==7.0.0b5"],
        "Backoff": ["backoff"],
    },
    url="https://github.com/calcbench/python_api_client",
    long_description="A client for Calcbench's API.  www.calcbench.com/api.  If you need Python 2 support install version 2.4.0",
)