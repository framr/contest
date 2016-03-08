#!/usr/bin/env python
__author__ = 'fram'
"""
Use two sequential models with an Embedding layer: one for the word, the other for the context.
Merge the two with a Merge layer in "dot" mode.
This gives you the dot product between a word embedding and a context embedding, which you can use to train
Mikolov-style word embeddings.
"""

from argparse import ArgumentParser
import numpy as np

from keras.models import Sequential
from keras.layers import Embedding, Merge
from keras.layers.core import Activation
from keras.preprocessing import sequence
from keras.optimizers import SGD


from streamer import SymmetricContextStreamer, SessionContextStreamer, stream_generator, Batch2BatchStreamer, SkipGramStreamer
from tabutils import RecordMeta
from preprocessing import read_sampling_table, read_feature_map


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_option('-i', dest="input", default=None, type=str, help="input file")
    parser.add_option('-d', '--dim', dest="dim", default=300, type=int, help="embeddings dimensionality")
    parser.add_option('-b', '--batch_size', dest="batch_size", default=50, type=int, help="training batch size")
    parser.add_option('--window', dest="window", default=5, type=int, help="window size")
    parser.add_option('--neg', dest="neg_pairs", default=1.0, type=float, help="neg samples proportion")
#    parser.add_option('-i', dest="input", default=None, type=str, help="input file")

    args = parser.parse_args()

    embedding_dim = args.dim
    num_features = ...

    word_layer = Sequential()
    word_layer.add(Embedding(num_features, embedding_dim, init='uniform'))
    context_layer = Sequential()
    context_layer.add(Embedding(num_features, embedding_dim, init='uniform'))
    model = Sequential()
    model.add(Merge([word_layer, context_layer], mode='dot', dot_axes=1))
    model.add(Activation('sigmoid'))

    sgd = SGD(lr=0.1, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='binary_crossentropy', optimizer=sgd)


    #instream = None

    meta = RecordMeta(open(args.input + '.meta').readline().strip().split())
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
        if couples:
            X1, X2 = zip(*couples)
            X1 = np.array(X1, dtype="int32")
            X2 = np.array(X2, dtype="int32")
            loss = model.train_on_batch([X1, X2], labels)















