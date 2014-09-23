#coding: utf-8

from ckanext.harvest.harvesters.base import HarvesterBase
from pprint import pprint, pformat
import json
import urllib2
import logging

from ckan import model
from ckan.model import Session, Package
from ckan.logic import get_action, action
from ckanext.harvest.model import HarvestObject
from ckan.lib.munge import munge_title_to_name

log = logging.getLogger(__name__)


class SalsahHarvester(HarvesterBase):
    '''
    SALSAH Harvester
    '''

    API_URL = "http://salsah.org/api/ckan?limit=3"
    
    ORGANIZATION = {
        'de': {
            'name': 'Digital Humanities Lab',
            'description': 'Description of Digital Humanities Lab',
            'website': 'http://www.dhlab.unibas.ch/index.php/de/'
        },
        'fr': {
            'name': 'Digital Humanities Lab',
            'description': 'Description of Digital Humanities Lab'
        },
        'it': {
            'name': 'Digital Humanities Lab',
            'description': 'Description of Digital Humanities Lab'
        },
        'en': {
            'name': 'Digital Humanities Lab',
            'description': 'Description of Digital Humanities Lab'
        }
    }

    config = {
        'user': u'harvest'
    }

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

    def _generate_resources_dict_array(self, resource):
        resource_list = []
        pages = resource.get('pages', [])
        for page in pages:
            resource_list.append({
                'name': self._get(page, 'incunabula:pagenum'),
                'resource_type': 'page',
                'url': self._get(page, 'salsah_url')
            })
        return resource_list

    def _find_or_create_groups(self, groups, context):
        log.debug('Group names: %s' % groups)
        group_ids = []
        for group_name in groups:
            data_dict = {
                'id': group_name,
                'name': munge_title_to_name(group_name),
                'title': group_name
            }
            try:
                group = get_action('group_show')(context, data_dict)
                log.info('found the group ' + group['id'])
            except:
                group = get_action('group_create')(context, data_dict)
                log.info('created the group ' + group['id'])
            group_ids.append(group['id'])

        log.debug('Group ids: %s' % group_ids)
        return group_ids

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
                # add dataset for project
                metadata = {
                    'datasetID': project['project_info'].get('shortname'),
                    'title': project['project_info'].get('longname'),
                    'url': 'http://salsah.org/',
                    'notes': "This project is part of SALSAH.",
                    # 'author': ,
                    # 'maintainer': ,
                    # 'maintainer-email': ,
                    # 'license_id': ,
                    # 'license_url': ,
                    # 'tags': ,
                    # 'resources': ,
                    'groups': [project['project_info'].get('ckan_category')],
                    'extras': [
                        ('level', 'Project')
                    ]
                }
                
                pprint(metadata)
                
                obj = HarvestObject(
                    guid=metadata['datasetID'],
                    job=harvest_job,
                    content=json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                ids.append(obj.id)

                for resource in project['project_resources']:
                    pages = resource.get('pages', [])
                    for page in pages:
                        # do we add JSON resources for pages?
                        pass

                    metadata = {
                        'datasetID': self._get(resource, 'resid').zfill(8),
                        'title': self._get(resource, 'dc:title', 'dokubib:titel'),
                        'url': self._get(resource, 'salsah_url', 'salsah:uri'),
                        # 'notes': self._get(resource, 'dc:description'),
                        'author': self._get(resource, 'dc:creator', 'dokubib:urheber'),
                        # 'maintainer': ,
                        # 'maintainer_email': ,
                        # 'license_id': 'cc-zero',
                        # 'license_url': 'http://opendefinition.org/licenses/cc-zero/',
                        # 'tags': ,
                        'resources': self._generate_resources_dict_array(resource),
                        'groups': [project['project_info'].get('ckan_category')],
                        'extras': [
                                ('level', 'Resource'),
                                ('parent', project['project_info'].get('shortname'))
                        ]
                    }

                    if type(metadata['author']) == dict:
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

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)

    def import_stage(self, harvest_object):
        log.debug('In SalsahHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False


        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = package_dict[u'datasetID']

            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }
            
            # Find or create the organization the dataset should get assigned to
            data_dict = {
                'permission': 'edit_group',
                'id': munge_title_to_name(self.ORGANIZATION['de']['name']),
                'name': munge_title_to_name(self.ORGANIZATION['de']['name']),
                'title': self.ORGANIZATION['de']['name'],
                'description': self.ORGANIZATION['de']['description'],
                'extras': [
                    {
                        'key': 'website',
                        'value': self.ORGANIZATION['de']['website']
                    }
                ]
            }
            try:
                package_dict['owner_org'] = get_action(
                    'organization_show')(context,
                                         data_dict)['id']
            except:
                organization = get_action(
                    'organization_create')(
                    context,
                    data_dict)
                package_dict['owner_org'] = organization['id']

            # Find or create group the dataset should get assigned to
            self._find_or_create_groups(package_dict['groups'], context)

            # Insert or update the package
            package = model.Package.get(package_dict['id'])
            model.PackageRole(
                package=package,
                user=user,
                role=model.Role.ADMIN
            )

            self._create_or_update_package(
                package_dict,
                harvest_object
            )

            Session.commit()
            
        except Exception as e:
            log.exception(e)
            raise
