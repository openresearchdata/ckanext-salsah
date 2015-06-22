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
from ckan.lib.munge import munge_title_to_name, munge_tag

log = logging.getLogger(__name__)


class SalsahHarvester(HarvesterBase):
    '''
    SALSAH Harvester
    '''

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

    config = None
    limit = 3
    user = u'harvest'

    def info(self):

        return {
            'name': 'salsah',
            'title': 'SALSAH',
            'description': 'SALSAH Harvester'
        }

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'limit' in self.config:
                self.limit = int(self.config['limit'])
            if 'user' in self.config:
                self.user = self.config['user']

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def _get_limit_param(self):
        return '?limit=%s' % self.limit

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

    def _generate_resources_dict_list(self, files_list):
        resource_list = []
        for file in files_list:
            resource_dict = {
                'name': self._get(file, 'ckan_title'),
                'resource_type': 'file',
                'url': self._get(file, 'data_url')
            }

            resource_dict.update(file)
            resource_list.append(resource_dict)
        return resource_list

    def _generate_tags_dict_list(self, tags_list):
        tags_dic_list = []
        for tag in tags_list:
            tag_dict = {
                'name': tag
            }
            tags_dic_list.append(tag_dict)
        return tags_dic_list

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
        self._set_config(harvest_job.source.config)

        # Get api url
        base_url = harvest_job.source.url.rstrip('/')
        base_api_url = base_url + self._get_limit_param()

        try:
            log.debug('Gathering data from: %s.' % base_api_url)
            req = urllib2.Request(base_api_url)
            handle = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            log.error('HTTP Error accessing %s: %s.' % (base_api_url, e.code))
            return []
        else:
            projects = json.load(handle)['projects']
            for project in projects:
                log.debug(project['project_info'])
                # add dataset for project
                metadata = {
                    'datasetID': self._get(project['project_info'], 'shortname'),
                    'title': self._get(project['project_info'], 'longname'),
                    'url': 'http://salsah.org/',
                    'notes': 'This project is part of SALSAH.',
                    # 'author': ,
                    # 'maintainer': ,
                    # 'maintainer-email': ,
                    # 'license_id': ,
                    # 'license_url': ,
                    'tags': [munge_tag(tag[:100]) for tag in self._get(project['project_info'], 'ckan_tags')],
                    'resources': [{
                        'name': 'SALSAH API',
                        'resource_type': 'api',
                        'url': harvest_job.source.url.rstrip('/') + '?project=' + self._get(project['project_info'], 'shortname')
                    }],
                    'groups': [self._get(project['project_info'], 'longname')],
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

                for resource in self._get(project, 'project_datasets'):

                    metadata = {
                        'datasetID': self._get(resource, 'resid').zfill(8),
                        'title': self._get(resource, 'ckan_title'),
                        'url': self._get(resource, 'salsah_url', 'salsah_uri'),
                        'notes': self._get(resource, 'ckan_description'),
                        'author': self._get(resource, 'dc_creator', 'dokubib_urheber'),
                        # 'maintainer': ,
                        # 'maintainer_email': ,
                        # 'license_id': 'cc-zero',
                        # 'license_url': 'http://opendefinition.org/licenses/cc-zero/',
                        'tags': [munge_tag(tag[:100]) for tag in self._get(resource, 'ckan_tags')],
                        'resources': self._generate_resources_dict_list(self._get(resource, 'files')),
                        'groups': [self._get(project['project_info'], 'longname')],
                        'extras': [
                                ('level', 'Resource'),
                                ('parent', self._get(project['project_info'], 'shortname'))
                        ]
                    }

                    if type(metadata['author']) == dict:
                        if 'salsah_firstname' in metadata['author']:
                            metadata['author'] = metadata['author']['salsah_lastname'] + ', ' + metadata['author']['salsah_firstname']
                        else:
                            metadata['author'] = metadata['author']['salsah_lastname']

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
            self._save_object_error(
                'Error in fetch stage: %s' % e,
                harvest_object
            )

    def import_stage(self, harvest_object):
        log.debug('In SalsahHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            self._save_object_error('No harvest object received')
            return False


        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = package_dict[u'datasetID']

            user = model.User.get(self.user)
            context = {
                'model': model,
                'session': Session,
                'user': self.user
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

            # Create relationship between package and parent
            for extra in package_dict['extras']:
                if extra[0] == 'parent':
                    data_dict = {
                        'subject': package_dict['id'],
                        'object': extra[1],
                        'type': 'child_of'
                    }

                    relationship = get_action('package_relationship_create')(
                        context,
                        data_dict
                    )

                    log.debug('Relationship created: %s' % str(relationship))

        except Exception as e:
            log.exception(e)
            self._save_object_error(
                'Exception in import stage: %s' % e,
                harvest_object
            )
            raise
