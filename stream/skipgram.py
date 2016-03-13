import sys
from argparse import ArgumentParser

from stream.streamer import SymmetricContextStreamer, SessionContextStreamer, stream_generator, Batch2BatchStreamer, SkipGramStreamer
from tabtools.tabutils import RecordMeta
from preprocess.preprocessing import read_sampling_table, read_feature_map


if __name__ == '__main__':

    argparser = ArgumentParser()
    argparser.add_option('-i', dest='infile', type=str, default=None, help='input file')
    argparser.add_option('-o', dest='outfile', type=str, default=None, help='output file')
    argparser.add_option('--del', dest='delete', type=int, default=None,
        help='keep items with at least del counts')
    argparser.add_option('--sub', dest='subsample', type=float, default=None,
        help="subsampling parameter in Mikolov's formula")
    argparser.add_option('--stats', dest='sampling_table', type=str, default=None,
        help="file with items statistics")
    argparser.add_option('--fmap', dest='feature_map', type=str, default=None,
        help="feature map file")
    argparser.add_option('--win', dest='window', type=int, default=None,
        help='window size')
    argparser.add_option('--weight', dest='win_weight', action='store_true',
        help='weigh contexts according to their distance from center word')
    argparser.add_option('--session', dest='session', action='store_true',
        help='generate contexts from entire session. this option revokes win and weight options')


    args = argparser.parse_args()

    meta_file = args.input + '.meta'
    print "Reading meta information from file %s" % meta_file
    meta = RecordMeta(open(meta_file).readline().strip().split())


    stream_gen = stream_generator(args.input, meta=meta, field=args.field, session=True)
    context_streamer = SessionContextStreamer(stream_gen)

    feature_map = read_feature_map(args.sampling_table)
    sampling_table = read_sampling_table(args.sampling_table)

    skipgram_streamer = SkipGramStreamer(context_streamer, neg_pairs=args.neg_pairs, window_size=args.window_size,
        feature_map=, sampling_table=)
    batch_streamer = Batch2BatchStreamer(skipgram_streamer, batch_size=args.batch_size)

    sampling_table = preprocessing.read_sampling_table()
    num_features = len(sampling_table)

    for i, seq in enumerate(streamer):
        # get skipgram couples for one text in the dataset
        couples, labels = sequence.skipgrams(seq)

