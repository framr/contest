#!/usr/bin/env python
__author__ = 'fram'

import os
import sys


from conda import split_learn_test
from tabutils import RecordMeta

if __name__ == '__main__':

    infile = sys.argv[1]
    meta_file = sys.argv[2]

    meta = RecordMeta(open(meta_file).read().strip().split())
    #period = ("2015-02-01 00:00:00", "2015-03-01 23:00:00")
    period = (sys.argv[3], sys.argv[4])

    split_learn_test(infile, meta, period, 1 * 60, 'learn_1min', 'test_1min')
