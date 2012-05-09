
from datetime import datetime
from optparse import make_option

from django.db import transaction
from django.core.management.base import BaseCommand
from pyes import ES, TermQuery

from analysis.corpus import Corpus
from analysis.ingestion import DocumentIngester
from analysis.parser import ngram_parser


def load_docket(docket):
    c = ES(['localhost:9200'])
        
    docs = list()
    
    for r in c.search(TermQuery("docket_id", docket))['hits']['hits']:
        text = "\n".join([file['text'].encode('ascii', 'replace') for file in r['_source']['files'] if len(file['text']) > 0])
        metadata = dict([(key, str(value)) for (key, value) in r['_source'].items() if key != 'files' and value is not None])
        docs.append(dict(text=text, metadata=metadata))
    
    return docs
    


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (make_option("-n", "--ngrams", dest="ngrams"),)

    def handle(self, agency, docket_id, **options):
        print "Loading data from ElasticSearch at %s" % datetime.now()
        
        docs = load_docket(docket_id)

        print "Beginning processing at %s" % datetime.now()

        with transaction.commit_on_success():
            c = Corpus(metadata=dict(docket=docket_id, agency=agency))
            if options.get('ngrams'):
                i = DocumentIngester(c, parser=ngram_parser(int(options['ngrams'])))
            else:
                i = DocumentIngester(c)
            i.ingest(docs)

        print "Finished processing at %s" % datetime.now()

        print "Added %d documents in corpus %d" % (len(docs), c.id)

    
    