#!/usr/bin/env python3

import csv
import sys

import ce_stats


def main():
    """main function
    """
    with ce_stats.GitRepo(ce_stats.GWMS_FACTORY_REPO) as repo:
        gwms_ces = ce_stats.get_gwms_ces(repo.path)

    ce_fqdns = set.union(gwms_ces, ce_stats.get_panda_ces())

    fields = ['HOSTNAME',
              'IDLE',
              'RUNNING',
              'REMOVED',
              'COMPLETED',
              'HELD',
              'TRANSFERRING_OUTPUT',
              'SUSPENDED',
              'COMMUNICATION_ERROR']
    writer = csv.DictWriter(sys.stdout, fieldnames=fields)
    writer.writeheader()

    for fqdn in ce_fqdns:
        row = {'HOSTNAME': fqdn}
        try:
            row.update(ce_stats.get_ce_jobs(fqdn))
        except Exception as exc:  # Failed communication with collector.
            row.update({status: 0 for status in ce_stats.CONDOR_STATUS_MAP.values()})
            row['COMMUNICATION_ERROR'] = str(exc)

        writer.writerow(row)


if __name__ == "__main__":
    main()
