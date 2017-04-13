#!/usr/bin/env python

from setuptools import setup

setup(name='dataengineer-toolkit',
        version='0.1',
        description='toolkit for data engineers on hdp',
        author='izhar firdaus',
        author_email='kagesenshi.87@gmail.com',
        license='bsd',
        url='http://github.com/kagesenshi/dataengineer_toolkit/',
        packages=['dataengineer_toolkit'],
        package_dir={'':'src'},
        entry_points={
            'console_scripts': [
                'oracle_profiler=dataengineer_toolkit.dbprofiler.oracle:main'
                ]
            },
        install_requires=[
            'sqlalchemy',
            'PyYAML',
            'PySocks',
            'Paramiko',
            'sshtunnel',
            'pyhs2',
            'Jinja2',
            'cx_Oracle'
        ],
        zip_safe=False
)
