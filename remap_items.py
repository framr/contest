#!/usr/bin/env python
"""
Replace offer_ids with

"""

import sys
from argparse import ArgumentParser

from preprocess.preprocessing import remap_items


if __name__ == '__main__':

    argparser = ArgumentParser()
    argparser.add_argument('-i', dest='infile', default=None, type=str, help='input file')
    argparser.add_argument('-o', dest='outfile', default=None, type=str, help='output file')
    argparser.add_argument('--fmap', dest='map_file', default=None, type=str, help='output feature map file')
    argparser.add_argument('--offer-field', dest='offer_field', default=None, type=str, help='field with list of offers')
    argparser.add_argument('--enumerate', dest='enumerate', action='store_true',
        help='replace items with dense integers, starting from 0')
    args = argparser.parse_args()

    remap_items(args.infile, args.outfile, args.map_file, offer_field=args.offer_field, enumerate=args.enumerate)
