#!/usr/bin/env python

import os

from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES
for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

with open(os.path.join('dqo','__version__.py')) as f:
  exec(f.read())

setup(name='dqo',
  version=__version__,
  description="Micro ORM for regular and async python code.",
  author='Derek Anderson',
  author_email='public@kered.org',
  packages = ['dqo'],
  url='https://github.com/keredson/dqo',
)
