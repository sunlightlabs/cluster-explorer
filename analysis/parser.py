
from collections import defaultdict
import nltk.data
import re

_sentence_breaker = nltk.data.load('tokenizers/punkt/english.pickle')

_non_words = re.compile('\W+')
def _normalize(token):
    words = re.sub(_non_words, ' ', token).lower()
    
    # filter phrases that aren't at least half word characters--likely represents non-content
    if len(words) <= len(token) / 2:
        return ''
        
    return words

def _boundary_sequencer(boundaries, text, sequencer):
    phrase_map = defaultdict(list)
    for (start, end) in boundaries:
        normalized = _normalize(text[start:end])
        if normalized != '':
            phrase_map[sequencer.sequence(normalized)].append((start, end))

    phrases = phrase_map.items()
    phrases.sort(key=lambda (id, indexes): id)

    return phrases


def _ngram_boundaries(text, n):
    result = list()
    unigram_boundaries = [match.span() for match in re.finditer('\w+', text)]

    for i in range(0, len(unigram_boundaries) + 1 - n):
        result.append((unigram_boundaries[i][0], unigram_boundaries[i + n - 1][1]))
        
    return result

def ngram_parser(n):
    return lambda text, squencer: _boundary_sequencer(_ngram_boundaries(text, n), sequencer)


def _sentence_boundaries(text):
    token_indexes = list()
    start = 0
    end = 0
    for t in _sentence_breaker.tokenize(text.strip()):
        start = end + text[end:].find(t)
        end = start + len(t)
        if t.strip() != '' and len(t) < 1000:
            token_indexes.append((start, end))
    
    return token_indexes
    
def sentence_parse(text, sequencer):
    return _boundary_sequencer(_sentence_boundaries(text), text, sequencer)
    
