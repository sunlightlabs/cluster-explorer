

import nltk.data

sentence_breaker = nltk.data.load('tokenizers/punkt/english.pickle')

def break_sentences(text):
    return sentence_breaker.tokenize(text)