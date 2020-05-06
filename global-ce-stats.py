#!/usr/bin/env python3

import global_ces


def main():
    """main function
    """
    ce_fqdns = set.union(global_ces.get_gwms_ces(), global_ces.get_panda_ces())

    global_ce_stats = dict()
    for fqdn in ce_fqdns:
        try:
            print('{0}: {1}'.format(fqdn, global_ces.get_ce_jobs(fqdn)))
        except Exception as exc:  # Failed communication with collector.
            print('{0}: {1}'.format(fqdn, exc))
            continue

    print(global_ce_stats)


if __name__ == "__main__":
    main()
