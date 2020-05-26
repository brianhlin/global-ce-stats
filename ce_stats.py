#!/usr/bin/env python3

"""Find HTCondor-CE stats using pilot factory configuration sources
"""

import json
import os
import shutil
import subprocess

from datetime import date
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


def _ce_fqdn(contact_str: str) -> str:
    """Given a CE contact string:
    - <FQDN>:<PORT>
    - <FQDN> <FQDN>:<PORT>
    Remove the port if it exists and return the CE FQDN as a string
    """
    return contact_str.split(':')[0].split()[0]


def _parse_gwms_config(config: ElementTree.ElementTree) -> Set:
    """Given a GlideinWMS factory config XML as an xml.ElementTree.Element, return the set of active HTCondor-CEs
    """
    gwms_entries = config.findall('./entries/entry')
    return {_ce_fqdn(entry.get('gatekeeper', ''))
            for entry in gwms_entries
            if entry.get('enabled', '') == 'True' and entry.get('gridtype', '') == 'condor'}


class GitRepo():
    """Class for handling Git repositories
    """

    def __init__(self, repo):
        """Clone a Git 'repo' into a temporary directory.
        The caller is responsible for cleaning up this directory.
        """
        self._old_workdir = os.getcwd()
        self.repo = repo
        self.path = mkdtemp()
        self._git_run_command('clone', self.repo, self.path)
        self.head = self._rev_list('-n1', '--first-parent', 'master')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.path)

    def _git_run_command(self, *args):
        """Run git subcommand with args against the GitRepo temporary directory, returning stdout
        """
        os.chdir(self.path)
        proc = subprocess.Popen(['git'] + list(args),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL)
        out, err = proc.communicate()
        os.chdir(self._old_workdir)

        if proc.returncode:
            raise RuntimeError(str(err))

        return out

    def _rev_list(self, *args):
        """Run git rev-list with args, returning stdout
        """
        return self._git_run_command('rev-list', *args)

    def checkout_at_date(self, date_obj: date):
        """Check out repo at midnight local time of the given date_obj.
        If date_obj is older than the repository, check out the earliest commit
        """
        earliest_commit = self._rev_list('-n1', '--max-parents=0', 'master')
        commit_at_date = self._rev_list('-n1',
                                        '--first-parent',
                                        '--before={0}'.format(date_obj.strftime('%Y-%m-%d')),
                                        'master')
        if not commit_at_date:
            commit_at_date = earliest_commit

        self._git_run_command('checkout', commit_at_date.strip())
        self.head = commit_at_date


def get_gwms_ces(repo_dir, production: bool = True) -> Set:
    """Given a Git repository containing GlideinWMS factory XML configuration,
    return the set of active, production HTCondor-CEs known to the GlideinWMS factories
    """
    config_glob = '{0}/*.xml'.format(repo_dir)
    factory_configs = glob(config_glob)
    if production:
        # Non-production entries are stored in '*-itb.xml'
        factory_configs = [filename for filename in factory_configs if '-itb.xml' not in filename]

    gwms_ces = set()
    for config_file in factory_configs:
        config_et = ElementTree.parse(config_file)
        gwms_ces.update(_parse_gwms_config(config_et))

    return gwms_ces


def get_panda_ces(panda_url: str = PANDA_URL) -> Dict[str, Set]:
    """Find list of active HTCondor-CEs, grouped by site name, known to the PanDA queues
    """
    panda_query = '?json&preset=schedconf.all&state=ACTIVE&ce_flavour=HTCONDOR-CE&is_production=TRUE'

    with request.urlopen(panda_url + panda_query) as response:
        panda_response = response.read()
        panda_resources = json.loads(panda_response)

    panda_ces: Dict[str, Set] = dict()
    for _, resource_info in panda_resources.items():
        site = resource_info['atlas_site']
        for queue in resource_info['queues']:
            endpoint = _ce_fqdn(queue['ce_endpoint'])
            try:
                panda_ces[site].update({endpoint})
            except KeyError:
                panda_ces[site] = set([endpoint])

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
