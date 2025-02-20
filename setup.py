#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-mixpanel',
      version='1.7.1',
      description='Singer.io tap for extracting data from the mixpanel API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mixpanel'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.3',
          'singer-python==6.0.0',
          'jsonlines==1.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-mixpanel=tap_mixpanel:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_mixpanel': [
              'schemas/*.json'
          ]
      })
