
import tempfile
import csv

from django.db import connection


class PhraseSequencer(object):

    def __init__(self, corpus_id):
        """Initialize the sequencer from stored phrases"""

        self.corpus_id = corpus_id

        c = connection.cursor()
        
        c.execute("select max(phrase_id) from phrases where corpus_id = %s", [corpus_id])
        result = c.fetchone()
        self.next_id = result[0] + 1 if result[0] is not None else 0
        
        c.execute("select phrase_text, phrase_id from phrases where corpus_id = %s", [corpus_id])
        self.phrase_map = dict(c)
        
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
        self.writer.writerow([self.corpus_id, phrase_id, phrase])
        
        return phrase_id 

    def upload_new_phrases(self):
        """Upload phrases created during use of sequencer"""
        
        self.new_phrase_file.flush()
        self.new_phrase_file.seek(0)
        
        connection.cursor().copy_from(self.new_phrase_file, 'phrases', sep=',')
        
        self.new_phrase_file.close()
        self.new_phrase_file = tempfile.TemporaryFile()
        self.writer = csv.writer(self.new_phrase_file)
