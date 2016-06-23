# -*- coding: utf-8 -*-
# @Author: ThomasO
from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='maestro',
    version='0.0.1',
    description='Spark cluster on the fly',
    long_description=readme,
    author='Thomas Opsomer',
    author_email='thomasopsomer.enpc@gmail.com',
    url='https://github.com/thomasopsomer/maestro',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)