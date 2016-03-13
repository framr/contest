import os
import sys
import csv

from collections import namedtuple

"""
Handmade utils for working with tab-separated files with named columns.
Looks ugly currently.
"""

def make_record_cls(fields):
    return namedtuple("Record", fields)


class RecordMeta(object):
    def __init__(self, fields):
        '''
        :param columns: mapping from column names to indices
        :param fields: list of column names (order is important)
        :return:
        '''
        self._fields = fields
        self._columns = dict((col, i) for i, col in enumerate(fields))

    def __getitem__(self, i):
        return self._columns[i]

    def fields(self):
        return self._fields

def dump_records(f, records):
    for r in records:
        f.write("%s\n" % "\t".join([getattr(r, field) for field in r._fields]))

def _grep(infile, col, min, max, outfile):

    with open(infile, 'r') as f:
        with open(outfile, 'w') as outf:
            for line in f:
                value = line.strip().split('\t', col)[col]
                if value < max and value > min:
                    outf.write(line)


def _grep2(infile, col, min, max, outfile):
    """
    csv based grep
    """
    with open(infile, 'r') as f:
        with open(outfile, 'w') as outf:

            reader = csv.reader(f, delimiter='\t')
            for line in reader:
                value = line[col]
                if value < max and value > min:
                    outf.write("%s\n" % '\t'.join(line))


def _grep3(infile, outfile):
    """
    complex expression grep
    """
    pass

if __name__ == '__main__':
    pass
