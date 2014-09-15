#coding: utf-8

from ckanext.harvest.harvesters.base import HarvesterBase
from pprint import pprint
import json
import urllib2
import logging

log = logging.getLogger(__name__)


class SalsahHarvester(HarvesterBase):
    '''
    SALSAH Harvester
    '''

    API_URL = "http://salsah.org/api/ckan?limit=3"

    def info(self):

        return {
            'name': 'salsah',
            'title': 'SALSAH',
            'description': 'SALSAH Harvester'
        }

    def _get_title(self, res):
        try:
            return res['dc:title']
        except KeyError:
            return res['dc:description']

    def gather_stage(self, harvest_job):
        log.debug('In SalsahHarvester gather_stage')
        try:
            req = urllib2.Request(self.API_URL)
            handle = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            log.debug('HTTP Error accessing %s: %s.' % (self.API_URL, e.code))
            return []
        else:
            projects = json.load(handle)['projects']
            for project in projects:
                log.debug(project['project_info'])
                for resource in project['project_resources']:
                    metadata = {
                        'datasetID': resource['resid'],
                        'title': self._get_title(resource),
                        # 'url': resource['salsah_url'],
                        # 'notes': resource['dc:description'] if resource.get('dc:description') else ''
                        # 'author': ,
                        # 'maintainer': ,
                        # 'maintainer_email': ,
                        # 'license_id': 'cc-zero',
                        # 'license_url': 'http://opendefinition.org/licenses/cc-zero/',
                        # 'tags': ,
                        # 'resources': ,
                        # 'extras': [
                        #         ('key', 'value)))
                        # ]
                    }
                    pprint(metadata)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In SalsahHarvester fetch_stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In SalsahHarvester import_stage')
        return True
