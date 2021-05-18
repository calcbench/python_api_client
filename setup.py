"""
Created on Mar 18, 2015


@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""
from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="calcbench_api_client",
    version="4.0.1",
    description="Client for Calcbench data.",
    author="Andrew Kittredge",
    author_email="andrew@calcbench.com",
    license="Apache2",
    keywords="finance accounting SEC 10-(K|Q)",
    install_requires=[
        "requests",
        "typing_extensions; python_version <= '3.7'",
        "dataclasses; python_version < '3.7'",
    ],
    python_requires=">=3.6",
    packages=["calcbench"],
    extras_require={
        "Pandas": ["Pandas"],
        "Listener": ["azure-servicebus==7.0.1"],
        "Backoff": ["backoff"],
    },
    url="https://github.com/calcbench/python_api_client",
    long_description=long_description,
    long_description_content_type="text/markdown",
)