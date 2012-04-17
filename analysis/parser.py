

import nltk.data
import re

sentence_breaker = nltk.data.load('tokenizers/punkt/english.pickle')

def break_sentences(text):
    return sentence_breaker.tokenize(text)


_non_words = re.compile('\W+')
def normalize(token):
    return re.sub(_non_words, '', token).lower()


def parse(text, sequencer):
    phrases = [sequencer.sequence(normalize(sentence)) for sentence in break_sentences(text)]
    phrases.sort()
    return phrases
