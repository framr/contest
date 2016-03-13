__author__ = 'fram'
from itertools import groupby
from datetime import datetime

class sessions_iter(object):
    '''
    Returns iterators to sessions identified as events displaced by less than max_interval in time
    '''
    def __init__(self, iterable, max_interval, key=None):

        if key is None:
            self.key = lambda r: datetime.strptime(r.timestamp, "%Y-%m-%d %H:%M:%S")
        self.it = iter(iterable)
        self.prev_time = self.cur_time = datetime(1970, 01, 01)
        self.max_interval = max_interval
        self.value = None

    def __iter__(self):
        return self

    def next(self):
        while (self.cur_time - self.prev_time).total_seconds() < self.max_interval:
            self.value = next(self.it)
            self.cur_time = self.key(self.value)
        self.prev_time = self.cur_time
        return self.cur_time, self._grouper()

    def _grouper(self):
        while (self.cur_time - self.prev_time).total_seconds() < self.max_interval:
            yield self.value
            self.prev_time = self.cur_time
            self.value = next(self.it)
            self.cur_time = self.key(self.value)

def group_sessions(records, max_interval):
    '''
    :param records: records iterator
    :param max_delay: maximum delay between events inside one action
    :return:
    '''

    current_session = []
    last_time = 0
    for rec in records:
        if not current_session or (rec.timestamp - last_time) < max_interval:
            current_session.append(rec)
        else: # new session
            yield
            current_session.append(rec)

    if current_session:
        yield

