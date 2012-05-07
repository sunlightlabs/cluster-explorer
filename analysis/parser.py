
from collections import defaultdict
import nltk.data
import re

_sentence_breaker = nltk.data.load('tokenizers/punkt/english.pickle')

_non_words = re.compile('\W+')
def normalize(token):
    return re.sub(_non_words, ' ', token).lower()

def sentence_parse(text, sequencer):
    sentences = [s for s in _sentence_breaker.tokenize(text) if len(s) < 1000] # long sentences are probably parse errors
    phrase_ids = list()
    for sentence in sentences:
        normalized = normalize(sentence)
        if normalized != '':
            phrase_ids.append(sequencer.sequence(normalized))
            
    return sorted(set(phrase_ids))


def ngram_parser(n):
    return lambda text, sequencer: ngram_parse(text, n, sequencer)

# a fake implementation--returns empty indexes
def ngram_indexed_parser(n):
    return lambda text, sequencer: [(id, []) for id in ngram_parse(text, n, sequencer)]


def ngram_parse(text, n, sequencer):
    normalized_text = re.sub('\W', ' ', text.lower())
    split_text = normalized_text.split()

    phrase_ids = list()
    
    for i in range(0, len(split_text) + 1 - n):
        phrase_ids.append(sequencer.sequence(" ".join(split_text[i:i+n])))
    
    return sorted(set(phrase_ids))



def sentence_boundaries(text):
    token_indexes = list()
    start = 0
    end = 0
    for t in _sentence_breaker.tokenize(text):
        start = end + text[end:].find(t)
        end = start + len(t)
        if t.strip() != '' and len(t) < 1000:
            token_indexes.append((start, end))
    
    return token_indexes
    
def sentence_indexed_parse(text, sequencer):
    phrase_map = defaultdict(list)
    for (start, end) in sentence_boundaries(text):
        normalized = normalize(text[start:end])
        if normalized != '':
            phrase_map[sequencer.sequence(normalized)].append((start, end))

    phrases = phrase_map.items()
    phrases.sort(key=lambda (id, indexes): id)
    
    return phrases