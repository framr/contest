import sys
from argparse import ArgumentParser
from math import sqrt


from stream.streamer import SymmetricContextStreamer, SessionContextStreamer, stream_generator, Batch2BatchStreamer, SkipGramStreamer
from tabtools.tabutils import RecordMeta
from preprocess.preprocessing import read_stats_table, read_feature_map


#XXX: Currently, there is no particular reason why this const is not zero.
WINDOW_CONTEXT_MARGIN = 5

def create_sampling_table(stats, min_freq=1e-5, min_shows=0, formula='mikolov'):
    """
    :param stats: statistics for each item
    :param min_freq:
    :param min_shows:
    :param formula:
    :return: table of probabilities for each word.
    """

    if formula != 'mikolov':
        raise NotImplementedError

    sampling_table = {}
    total_count = 0
    for item, count in stats.iteritems():
        total_count += count

    for item, count in stats.iteritems():
        freq = float(count) / total_count
        sampling_table[item] = 1 - sqrt(min_freq / freq) # Mikolov's formula

        if count < min_shows:
            sampling_table[item] = 0

    return sampling_table

if __name__ == '__main__':

    argparser = ArgumentParser()
    argparser.add_option('-i', dest='infile', type=str, default=None, help='input file')
    argparser.add_option('-o', dest='outfile', type=str, default=None, help='output file')
    argparser.add_option('--del', dest='min_shows', type=int, default=None,
        help='keep items with at least del counts')
    argparser.add_option('--min-freq', dest='sampling_min_freq', type=float, default=None,
        help='min_freq used in subsampling probabilities')
    argparser.add_option('--sub', dest='subsample', type=float, default=None,
        help="subsampling parameter in Mikolov's formula")
    argparser.add_option('--stats', dest='stats_table', type=str, default=None,
        help="file with items statistics")
    #argparser.add_option('--fmap', dest='feature_map', type=str, default=None,
    #    help="feature map file")
    argparser.add_option('--win', dest='window', type=int, default=None,
        help='window size')
    argparser.add_option('--weight', dest='win_weight', action='store_true',
        help='weigh contexts according to their distance from center word')
    argparser.add_option('--session', dest='session', action='store_true',
        help='generate contexts from entire session. this option revokes win and weight options')
    argparser.add_option('--neg', dest='neg_pairs', default=None, type=float,
        help='proportion of negative relative to positive pairs')

    args = argparser.parse_args()

    meta_file = args.input + '.meta'
    print "Reading meta information from file %s" % meta_file
    meta = RecordMeta(open(meta_file).readline().strip().split())


    stream_gen = stream_generator(args.input, meta=meta, field=args.field, session=args.session)
    context_streamer = None
    if args.session:
        context_streamer = SessionContextStreamer(stream_gen)
    else:
        context_streamer = SymmetricContextStreamer(stream_gen, context_hsize=args.window + WINDOW_CONTEXT_MARGIN)


    #feature_map = read_feature_map(args.sampling_table)
    stats_table = read_stats_table(args.sampling_table)
    sampling_table = create_sampling_table(stats_table, min_freq=args.sampling_min_freq, min_shows=args.min_shows)

    skipgram_streamer = SkipGramStreamer(context_streamer, neg_pairs=args.neg_pairs, window_size=args.window,
        sampling_table=sampling_table)

    sampling_table = preprocessing.read_sampling_table()
    num_features = len(sampling_table)

    for i, seq in enumerate(streamer):
        # get skipgram couples for one text in the dataset
        couples, labels = sequence.skipgrams(seq)





