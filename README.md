# CKAN Harvester for SALSAH

## Config options

The following config options can be set in the `Configuration`-field of the harvester:

### limit

* Type: `Integer`
* Default: `3`

### user

* Type: `String`
* Default: `harvest`

### Example of options usage:
```javascript
{
  "limit": 3,
  "user": "harvest"
}
```

## Developing without running jobs manually

It's quite awkward and slow to run the harvesters on the command line when you are developing a harvester. To solve this issue I have setup the tests from the ckan harvester and tweaked them a bit so we can use them to get feedback quickly on what we are doing.

The important stuff is located in `ckanext-salsah/ckanext/salsah/tests/test_queue.py`

A `TestSalsahHarvester` inherits from `SalsahHarvester`. Only the info is mocked, the other methods are kept. `TestHarvestqueue` then calls e.g. `gather_stage` from the original harvester. You can then either mock methods in the test or operate on the real harvester and run the tests to see what it does:

    . ~/default/bin/activate
    cd /vagrant/ckanext-salsah

    nosetests --logging-filter=ckanext.salsah.harvester --ckan --with-pylons=test.ini ckanext/salsah/tests

In this example the logging filter is used to only show messages of the harvester.
