
import tempfile
import csv


class PhraseSequencer(object):

    def __init__(self, corpus):
        """Initialize the sequencer from stored phrases"""

        self.corpus = corpus
        
        max_phrase_id = self.corpus.max_phrase_id()
        self.next_id = max_phrase_id + 1 if max_phrase_id is not None else 0
        
        self.phrase_map = self.corpus.all_phrases()
        
        self.new_phrase_file = tempfile.TemporaryFile()
        self.writer = csv.writer(self.new_phrase_file)

    def sequence(self, phrase):
        """Return a unique integer for the phrase
        
        If phrase is new, record for later upload to database.
        
        """

        phrase_id = self.phrase_map.get(phrase, None)
        if phrase_id is not None:
            return phrase_id
        
        phrase_id = self.next_id
        self.next_id += 1

        self.phrase_map[phrase] = phrase_id
        self.writer.writerow([self.corpus.id, phrase_id, phrase])
        
        return phrase_id 

    def upload_new_phrases(self):
        """Upload phrases created during use of sequencer"""
        
        self.new_phrase_file.flush()
        self.new_phrase_file.seek(0)
        
        self.corpus.upload_csv(self.new_phrase_file, 'phrases')
        
        self.new_phrase_file.close()
        self.new_phrase_file = tempfile.TemporaryFile()
        self.writer = csv.writer(self.new_phrase_file)
