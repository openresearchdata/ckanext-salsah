from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-salsah',
    version=version,
    description="Harvester for SALSAH ",
    long_description="""\
    CKAN Harvester for SALSAH (System for Annotation and Linkage of Sources in Arts and Humanities)
    """,
    classifiers=[],
    keywords='',
    author='Liip AG',
    author_email='ogd@liip.ch',
    url='http://www.liip.ch',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.salsah'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points=
    """
    [ckan.plugins]
    salsah_harvester=ckanext.salsah.harvester:SalsahHarvester
    [paste.paster_command]
    harvester=ckanext.salsah.command:SalsahHarvesterCommand
    """,
)
