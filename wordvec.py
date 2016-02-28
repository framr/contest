#!/usr/bin/env python
__author__ = 'fram'
"""
Use two sequential models with an Embedding layer: one for the word, the other for the context.
Merge the two with a Merge layer in "dot" mode.
This gives you the dot product between a word embedding and a context embedding, which you can use to train
Mikolov-style word embeddings.
"""

from keras.models import Sequential
from keras.layers import Embedding, Merge
from keras.preprocessing import sequence

word = Sequential()
word.add(Embedding(max_feature, vector_dim, init='uniform'))
context = Sequential()
context.add(Embedding(max_feature,vector_dim, init='uniform'))
model = Sequential()
model.add(Merge([word, context], mode='dot'))
model.compile(loss='mse', optimizer='rmsprop')


sampling_table = sequence.make_sampling_table(max_feature)



for i, seq in enumerate(tokenizer.texts_to_sequences_generator(text_generator())):
   # get skipgram couples for one text in the dataset
   couples, labels = sequence.skipgrams(seq, max_feature, window_size=4, negative_samples=1., sampling_table=sampling_table)
   if couples:
      X1,X2 = zip(*couples)
      X1 = np.array(X1,dtype="int32")
      X2 = np.array(X2,dtype="int32")
      loss = model.train_on_batch([X1,X2], labels)