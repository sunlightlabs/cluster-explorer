

import nltk.data
import re

sentence_breaker = nltk.data.load('tokenizers/punkt/english.pickle')

def break_sentences(text):
    return sentence_breaker.tokenize(text)


_non_words = re.compile('\W+')
def normalize(token):
    return re.sub(_non_words, '', token).lower()


def parse(text, sequencer):
    sentences = [s for s in break_sentences(text) if len(s) < 1000] # long sentences are probably parse errors
    phrase_ids = list()
    for sentence in sentences:
        normalized = normalize(sentence)
        if normalized != '':
            phrase_ids.append(sequencer.sequence(normalized))
            
    return sorted(set(phrase_ids))
