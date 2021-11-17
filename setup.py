#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-mixpanel',
      version='1.4.0',
      description='Singer.io tap for extracting data from the mixpanel API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mixpanel'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.12.1',
          'jsonlines==1.2.0'
      ],
      extras_require={
        'dev': [
            'pytest',
            'requests_mock',
        ]
      },
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
