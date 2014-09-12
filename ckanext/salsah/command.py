import sys
import re
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError

from ckanext.harvest.commands.harvester import Harvester


class SalsahHarvesterCommand(Harvester):
    """
    SALSAH Harvester command
    """
