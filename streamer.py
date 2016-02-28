import sys
from collections import deque

class BatchStreamer(object):
    pass

class PastBatchStreamer(object):
    pass

class SymmetricBatchStreamer(object):
    """
    class buffering part of the stream
    """
    def __init__(self, event_stream, batch_hsize=10):

        self.batch = deque()
        self.pos = self.left = self.right = 0
        self.batch_hsize = batch_hsize # batch half size
        self.instream = iter(event_stream)

    def __iter__(self):
        return self

    def next(self):
        while self.right - self.pos < self.batch_hsize + 1:
            try:
                self.batch.append(next(self.instream))
                self.right += 1
            except StopIteration:
                break

        while self.pos - self.left > self.batch_hsize:
            self.batch.popleft()
            self.left += 1

        if self.pos > self.right - 1 or not self.batch:
            # nothing to process
            raise StopIteration


        shift = self.pos - self.left
        self.pos += 1
        return (shift, self.batch)
        #self.pos += 1


if __name__ == '__main__':

    tests = [
        [],
        [1],
        [1, 2],
        [1, 2, 3, 4, 5],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ]

    for s in tests:
        b = SymmetricBatchStreamer(s, 1)
        print "stream ", s
        for p, batch in b:
            print p, list(batch)

