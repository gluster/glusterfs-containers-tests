#!/usr/bin/python
from setuptools import setup, find_packages

version = '0.1'
name = 'cns-tools'

setup(
    name=name,
    version=version,
    description='Red Hat Container-Native Storage Tools',
    author='Red Hat, Inc.',
    author_email='cns-qe@redhat.com',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha'
        'Intended Audience :: QE, Developers'
        'Operating System :: POSIX :: Linux'
        'Programming Language :: Python'
        'Programming Language :: Python :: 2'
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 2.7'
        'Topic :: Software Development :: Testing'
    ],
    install_requires=['glusto'],
    dependency_links=['http://github.com/loadtheaccumulator/glusto/tarball/master#egg=glusto'],
)
