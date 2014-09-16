#coding: utf-8

from ckanext.harvest.harvesters.base import HarvesterBase
from pprint import pprint, pformat
import json
import urllib2
import logging

from ckanext.harvest.model import HarvestObject

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

    def _get(self, resource, *args):
        """
        Try to return the value for key(s).
        If no key exists log the resource and the last key.
        """
        for key in args[:-1]:
            try:
                return resource[key]
            except KeyError:
                pass
        try:
            return resource[args[-1]]
        except KeyError:
            log.error('Resource does not contain `%s`:\n%s', args[-1], pformat(resource))
            return ''

    def gather_stage(self, harvest_job):
        log.debug('In SalsahHarvester gather_stage')
        ids = []
        try:
            req = urllib2.Request(self.API_URL)
            handle = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            log.error('HTTP Error accessing %s: %s.' % (self.API_URL, e.code))
            return []
        else:
            projects = json.load(handle)['projects']
            for project in projects:
                log.debug(project['project_info'])
                # TODO: add dataset for project
                for resource in project['project_resources']:
                    pages = resource.get('pages', [])
                    for page in pages:
                        pass
                        # do we add JSON resources for pages?
                    
                    metadata = {
                        'datasetID': self._get(resource, 'resid'),
                        'title': self._get(resource, 'dc:title', 'dokubib:titel'),
                        'url': self._get(resource, 'salsah_url', 'salsah:uri'),
                        # 'notes': self._get(resource, 'dc:description'),
                        'author': self._get(resource, 'dc:creator', 'dokubib:urheber'),
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

                    if type(metadata['author']) == dict:
                        #pprint(metadata['author'])
                        if 'salsah:firstname' in metadata['author']:
                            metadata['author'] = metadata['author']['salsah:lastname'] + ', ' + metadata['author']['salsah:firstname']
                        else:
                            metadata['author'] = metadata['author']['salsah:lastname']

                    pprint(metadata)

                    obj = HarvestObject(
                        guid=metadata['datasetID'],
                        job=harvest_job,
                        content=json.dumps(metadata)
                    )
                    obj.save()
                    log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                    ids.append(obj.id) 
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In SalsahHarvester fetch_stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In SalsahHarvester import_stage')
        return True
