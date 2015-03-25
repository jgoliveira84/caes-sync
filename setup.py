# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="caes-sync",
    version="0.1.0",
    author="João Gabriel Oliveira",

    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,

    entry_points={'console_scripts':
                  ['caes-sync-daemon = caes.sync:sync']},

    install_requires=[
        'cassandra-driver==2.1.4',
        'elasticsearch==1.4.0',
        'PyYAML==3.11' ,
        'blist==1.3.6',
        'python-daemon'
    ],

    test_suite='caes.test'
)