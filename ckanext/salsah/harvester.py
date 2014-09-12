from ckanext.harvest.harvesters.base import HarvesterBase

import logging
log = logging.getLogger(__name__)


class SalsahHarvester(HarvesterBase):
    '''
    SALSAH Harvester
    '''

    def info(self):

        return {
            'name': 'salsah',
            'title': 'SALSAH',
            'description': 'SALSAH Harvester'
        }

    def gather_stage(self, harvest_job):
        log.debug('In SalsahHarvester gather_stage')
        ids = []
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In SalsahHarvester fetch_stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In SalsahHarvester import_stage')
        return True
