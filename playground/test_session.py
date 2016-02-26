#!/usr/bin/env python
__author__ = 'fram'

import sys
import os

from session import sessions_iter
from tabutils import make_record_cls

if __name__ == '__main__':

    test_data = [
        ['2015-01-02 01:00:00',
         '2015-01-02 02:00:00',
         '2015-02-15 12:00:00', '2015-02-15 12:00:30', '2015-02-15 12:01:00',
         '2015-02-15 12:10:00', '2015-02-15 12:13:25'],
        []
    ]

    Record = make_record_cls(['timestamp'])
    for uid_session in test_data:
        print "Events: %s" % uid_session
        test_records = [Record(timestamp) for timestamp in uid_session]
        print "max time displacement 60s"
        for tstart, it in sessions_iter(test_records, 60 * 1):
            s = list(it)
            print "session length = %d tstart = %s,  %s" % (len(s), tstart, " ".join([r.timestamp for r in s]))
        print "max time displacement 5min"
        for tstart, it in sessions_iter(test_records, 60 * 5):
            s = list(it)
            print "session length = %d tstart = %s,  %s" % (len(s), tstart, " ".join([r.timestamp for r in s]))
        print "max time displacement 1h"
        for tstart, it in sessions_iter(test_records, 60 * 60):
            s = list(it)
            print "session length = %d tstart = %s,  %s" % (len(s), tstart, " ".join([r.timestamp for r in s]))
