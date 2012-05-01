from datetime import datetime
import json

from django.db import transaction
from django.core.management.base import BaseCommand

from analysis.corpus import Corpus
from analysis.ingestion import DocumentIngester

class Command(BaseCommand):
    
    def handle(self, ls_docs_path, **options):
        docs = list()
        for doc in json.load(open(ls_docs_path, 'r')):
            docs += [d['text'] for d in doc['documents'] if d.get('text')]

        cleaned_docs = [d.encode('ascii', 'replace') for d in docs]

        print "Beginning processing at %s" % datetime.now()

        with transaction.commit_on_success():
            c = Corpus()
            i = DocumentIngester(c)
            i.ingest(cleaned_docs)

        print "Finished processing at %s" % datetime.now()
        
        print "Added %d documents in corpus %d" % (len(cleaned_docs), c.id)


