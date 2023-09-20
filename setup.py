"""
Created on Mar 18, 2015


@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""
from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

from calcbench import __version__

setup(
    name="calcbench_api_client",
    version=__version__,
    description="Client for Calcbench data.",
    author="Andrew Kittredge",
    author_email="andrew@calcbench.com",
    license="Apache2",
    keywords="finance accounting SEC 10-(K|Q)",
    install_requires=[
        "requests",
        "typing_extensions; python_version <= '3.7'",
        "dataclasses; python_version < '3.7'",
        "pydantic",
    ],
    python_requires=">=3.6",
    packages=find_packages(),
    extras_require={
        "Pandas": ["Pandas>=1.0.0"],
        "Listener": ["azure-servicebus==7.2.0", "pytz"],
        "Backoff": ["backoff"],
        "BeautifulSoup": ["beautifulsoup4"],
        "tqdm": ["tqdm"],
        "Keyring": ["keyring"],
        "pyarrow": ["pyarrow"],
    },
    url="https://github.com/calcbench/python_api_client",
    project_urls={
        "Documentation": "http://calcbench.github.io/python_api_client/html/index.html",
        "Examples": "https://github.com/calcbench/notebooks",
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    copyright="Calcbench",
)
