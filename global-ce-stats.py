#!/usr/bin/env python3

import csv
import sys

import global_ces


def main():
    """main function
    """
    ce_fqdns = set.union(global_ces.get_gwms_ces(), global_ces.get_panda_ces())

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
            row.update(global_ces.get_ce_jobs(fqdn))
        except Exception as exc:  # Failed communication with collector.
            row.update({status: 0 for status in global_ces.CONDOR_STATUS_MAP.values()})
            row['COMMUNICATION_ERROR'] = str(exc)

        writer.writerow(row)


if __name__ == "__main__":
    main()
