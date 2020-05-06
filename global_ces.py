#!/usr/bin/env python3

"""Find global HTCondor-CEs from pilot factory configuration sources
"""

import json
import shutil
import subprocess

from glob import glob
from xml.etree import ElementTree
from tempfile import mkdtemp
from typing import Dict, Set
from urllib import request

import htcondor

CE_DEFAULT_PORT = 9619
GWMS_FACTORY_REPO = 'https://github.com/opensciencegrid/osg-gfactory'
PANDA_URL = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/'

# HTCondor job status mapping
CONDOR_STATUS_MAP = {1: 'IDLE',
                     2: 'RUNNING',
                     3: 'REMOVED',
                     4: 'COMPLETED',
                     5: 'HELD',
                     6: 'TRANSFERRING_OUTPUT',
                     7: 'SUSPENDED'}


def _ce_fqdn(ce_endpoint: str) -> str:
    """Given a CE contact string, remove the port if it exists and return the CE FQDN as a string
    """
    return ce_endpoint.split(':')[0]


def _parse_gwms_config(config: ElementTree.ElementTree) -> Set:
    """Given a GlideinWMS factory config XML as an xml.ElementTree.Element, return the set of active HTCondor-CEs
    """
    gwms_entries = config.findall('./entries/entry')
    return {_ce_fqdn(entry.get('gatekeeper', ''))
            for entry in gwms_entries
            if entry.get('enabled', '') == 'True' and entry.get('gridtype', '') == 'condor'}


def get_gwms_ces(config_repo: str = GWMS_FACTORY_REPO) -> Set:
    """Find list of active HTCondor-CEs known to the GlideinWMS factories
    """
    tmpdir = mkdtemp()
    cmd = ['git', 'clone', '--depth', '1', config_repo, tmpdir]
    subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    config_glob = '{0}/*.xml'.format(tmpdir)
    factory_configs = glob(config_glob)

    gwms_ces = set()
    for config_file in factory_configs:
        config_et = ElementTree.parse(config_file)
        gwms_ces.update(_parse_gwms_config(config_et))

    shutil.rmtree(tmpdir, ignore_errors=True)

    return gwms_ces


def get_panda_ces(panda_url: str = PANDA_URL) -> Set:
    """Find list of active HTCondor-CEs known to the PanDA queues
    """
    panda_query = '?json&preset=schedconf.all&state=ACTIVE&ce_flavour=HTCONDOR-CE&is_production=TRUE'

    with request.urlopen(panda_url + panda_query) as response:
        panda_response = response.read()
        panda_resources = json.loads(panda_response)

    panda_ces = set()
    for _, resource_info in panda_resources.items():
        for queue in resource_info['queues']:
            panda_ces.update({_ce_fqdn(queue['ce_endpoint'])})

    return panda_ces


def get_ce_jobs(fqdn: str) -> Dict:
    """Given an HTCondor-CE FQDN, return a dict containing the number of idle, running, held, and completed jobs
    """
    coll = htcondor.Collector(fqdn + ':' + str(CE_DEFAULT_PORT))
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
    schedd = htcondor.Schedd(schedd_ad)

    job_stats = {status: 0 for status in CONDOR_STATUS_MAP.values()}
    for job in schedd.xquery(requirements='RoutedJob =!= True)', projection=['JobStatus']):
        job_code = job['JobStatus']
        job_status = CONDOR_STATUS_MAP[job_code]
        job_stats[job_status] += 1

    return job_stats
