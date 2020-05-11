#!/usr/bin/env python3

"""Find the number of active, production HTCondor-CEs per month from the GlideinWMS factory configuration
"""

import calendar
import csv
from datetime import date
import sys

import ce_stats


def increment_month(sourcedate):
    """Add a month to the given date
    """
    year = sourcedate.year + sourcedate.month // 12
    month = sourcedate.month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def main():
    """main function
    """
    query_date = date(2016, 5, 1)  # start of factory config repo history containing entries
    writer = csv.DictWriter(sys.stdout, fieldnames=['MONTH', 'HTCONDOR_CE_COUNT'])
    writer.writeheader()

    with ce_stats.GitRepo(ce_stats.GWMS_FACTORY_REPO) as repo:
        while query_date < date.today():
            repo.checkout_at_date(query_date)

            row = {'MONTH': query_date.strftime('%Y-%m-%d'),
                   'HTCONDOR_CE_COUNT': len(ce_stats.get_gwms_ces(repo.path))}
            writer.writerow(row)

            query_date = increment_month(query_date)


if __name__ == "__main__":
    main()
