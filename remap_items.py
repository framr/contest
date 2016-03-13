#!/usr/bin/env python
"""
Replace offer_ids with

"""

import sys
from argparse import ArgumentParser

from preprocess.preprocessing import remap_items


if __name__ == '__main__':

    argparser = ArgumentParser()
    argparser.add_option('-i', dest='infile', default=None, type='str', help='input file')
    argparser.add_option('-o', dest='outfile', default=None, type='str', help='output file')
    argparser.add_option('--fmap', dest='map_file', default=None, type='str', help='output feature map file')

    args = argparser.parse_args()

    remap_items(args.infile, args.outfile, args.map_file)
