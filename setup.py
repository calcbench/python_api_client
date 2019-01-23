'''
Created on Mar 18, 2015


@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
'''
from setuptools import setup

setup(name='calcbench', 
      version='0.1.3', 
      description='Client for Calcbench data.', 
      author='Andrew Kittredge',
      author_email='andrew@calcbench.com',
      license='Apache2',
      keywords='finance accounting SEC 10-(K|Q)',
      install_requires=['requests'],
      packages=['calcbench'],
      extras_require={
            'Pandas':['Pandas'],
            'Listener':['azure-servicebus==0.21.1']
                  }
      )
