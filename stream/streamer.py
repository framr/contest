

import sys
from collections import deque
import random
import os
from itertools import chain
import numpy as np


from tabtools.tabutils import make_record_cls

def stream_generator(filename, meta=None, field=None, session=False):
    """
    :param filename: input filename
    :param meta: meta class
    :param field: field to extract
    :param session: boolean variable indicating, whether to group items into sessions
    :return: generate sequence of items or lists of items
    """

    Record = make_record_cls(meta.fields())
    for line in open(filename):
        splitted = line.strip().split('\t')
        rec = Record(*splitted)
        value = getattr(rec, field)
        items = value.split()
        if session:
            yield items
        else:
            for it in items:
                yield it


class ContextStreamer(object):
    pass

class PastContextStreamer(object):
    pass

class SymmetricContextStreamer(object):
    """
    class buffering part of the stream, for each event returns some context around it
    """
    def __init__(self, instream, context_hsize=10):

        self.context = deque()
        self.pos = self.left = self.right = 0
        self.context_hsize = context_hsize # context half size
        self.instream = iter(instream)

    def __iter__(self):
        return self

    def next(self):
        """
        :return: tuple - (position of current element in context, buffered batch of records)
        """
        while self.right - self.pos < self.context_hsize + 1:
            try:
                self.context.append(next(self.instream))
                self.right += 1
            except StopIteration:
                break

        while self.pos - self.left > self.context_hsize:
            self.context.popleft()
            self.left += 1

        if self.pos > self.right - 1 or not self.context:
            # nothing to process
            raise StopIteration


        shift = self.pos - self.left
        self.pos += 1
        return (shift, self.context)
        #self.pos += 1

class SessionContextStreamer(object):
    """
    class buffering part of the stream, for each event returns some context around it
    """
    def __init__(self, instream):
        self.instream = iter(instream)
        self.pos = 0
        self.session = []

    def __iter__(self):
        return self

    def next(self):
        """
        :return: tuple - (position of current element in context, buffered batch of records)
        """

        if self.pos == len(self.session):
            self.session = []
            self.pos = 0
            while not self.session:
                event = next(self.instream)
                self.session = list(event)

        shift = self.pos
        self.pos += 1
        return (shift, self.session)


class TokenizeTextStreamer(object):
    def __init__(self, instream, feature_map):
        pass
    def text_to_sequence(self):
        pass
    def __iter__(self):
        return self
    def __next__(self):
        pass



class SkipGramStreamer(object):
    def __init__(self, context_streamer, neg_pairs=1.0, window_size=5, min_window_size=None,
        sampling_table=None, neg_sampling_table=None, shuffle=True, min_shows=None):
        """
        :param context_streamer:
        :param feature_map: if provided, remap words according to mapping
        :return: tuples (word, context, distance between context and word positions)
        """

        self.shuffle = shuffle
        self.neg_pairs = neg_pairs
        self.window_size = window_size
        self.min_window_size = min_window_size
        self.sampling_table = sampling_table
        self.context_streamer = context_streamer

    def __iter__(self):
        return self
    def next(self):
        good_context = False
        while not good_context:
            center_pos, context = next(self.context_streamer)
            center_word = context[center_pos]

            good_context = True
            if len(context) < 2:
                good_context = False
            if self.sampling_table:
                if self.sampling_table[center_word] < random.random():
                    good_context = False
        #print "word, context %s, %s" % (center_word, context)

        # define window around pos
        if self.min_window_size is None:
            window_size = self.window_size
        else:
            window_size = random.randint(self.min_window_size, self.window_size)
        window_begin = max(center_pos - window_size, 0)
        window_end = min(center_pos + window_size, len(context) - 1)

        # aggregates positive pairs
        pairs = []
        for pos in range(window_begin, window_end + 1):
            if pos != center_pos:
                pairs.append((center_word, context[pos], abs(pos - center_pos)))

        num_positives = len(pairs)
        labels = len(pairs) * [1]

        num_negatives = int(num_positives * self.neg_pairs)

        negatives = []
        multi = np.random.multinomial(num_negatives, self.neg_sampling_table[0])
        for index in np.nonzero(multi):
            negatives.extend(
                [(center_word, self.neg_sampling_table[1][index], 0) for i in range(multi[index])]
            )



        labels += len(negatives) * [0]
        pairs += negatives

        if self.shuffle:
            tmp = zip(pairs, labels)
            random.shuffle(tmp)
            pairs, labels = zip(*tmp)

        return pairs, labels


class Batch2BatchStreamer(object):
    """
    Transform input stream containing from batches into batches of specified size (or greater)
    """
    def __init__(self, instream, batch_size):
        self.instream = instream
        self.batch_size = batch_size
    def __iter__(self):
        return self
    def next(self):
        batch = []
        while len(batch) < self.batch_size:
            try:
                batch.extend(next(self.instream))
            except StopIteration:
                break

        if not batch:
            raise StopIteration
        return batch




if __name__ == '__main__':

    tests = [
        [],
        [1],
        [1, 2],
        [1, 2, 3, 4, 5],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ]

    print "context size = 1"
    for s in tests:
        b = SymmetricContextStreamer(s, 1)
        print "stream ", s
        for p, context in b:
            print p, list(context)

    print "context size = 2"
    for s in tests:
        b = SymmetricContextStreamer(s, 2)
        print "stream ", s
        for p, context in b:
            print p, list(context)



    print "Session streamer"
    b = SessionContextStreamer(tests)
    for p, context in b:
        print p, list(context)


    stream = [["a", "b"], ["c", "d", "e"], ["f"]]
    flattened = list(chain.from_iterable(stream))
    feature_map = dict((w, i) for i, w in enumerate(flattened))
    sampling_table = dict(zip(flattened,  len(flattened) * [1.0]))
    print stream, feature_map
    print "Testing skipgrams"

    print "window size = 1"
    streamer = SessionContextStreamer(stream)
    skipgram = SkipGramStreamer(streamer, neg_pairs=1.0, window_size=1, feature_map=feature_map, sampling_table=sampling_table)
    for pairs in skipgram:
        print pairs

    print "window size = 2"
    streamer = SessionContextStreamer(stream)
    sampling_table = dict(zip(flattened,  len(flattened) * [0.5]))
    skipgram = SkipGramStreamer(streamer, neg_pairs=1.0, window_size=2, feature_map=feature_map, sampling_table=sampling_table)
    for pairs, labels in skipgram:
        print pairs, labels
        X1, X2 = zip(*pairs)
        import numpy as np
        X1 = np.array(X1, dtype="int32")
        X2 = np.array(X2, dtype="int32")
        print [X1, X2]



