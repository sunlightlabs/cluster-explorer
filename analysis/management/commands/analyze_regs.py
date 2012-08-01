from datetime import datetime
from optparse import make_option

from django.db import transaction
from django.core.management.base import BaseCommand

from analysis.corpus import Corpus
from analysis.ingestion import DocumentIngester
from analysis.parser import ngram_parser, sentence_parse

from regs_models import Docket, Doc


def doc_text(doc):
	return "\n".join([view.as_text() for view in doc.views])

def doc_metadata(doc):
	return {
		'id': doc.id,
		'title': doc.title,
		'agency': doc.agency,
		'docket_id': doc.docket_id,
		'type': doc.type,
		'created': unicode(doc.created),
		'last_seen': unicode(doc.last_seen)
	}


def ingest_docket(docket):
    print "Loading docket %s at %s..." % (docket.id, datetime.now())
    
    docs = [{
    		'text': doc_text(d),
    		'metadata': doc_metadata(d)
    		} for d in Doc.objects(docket_id=docket.id)]

    print "Loaded %s documents..." % len(docs)

    print "Beginning sentence ingestion at %s..." % datetime.now()
    c1 = Corpus(metadata=dict(docket=docket.id, agency=docket.agency, parser='sentence'))
    i1 = DocumentIngester(c1, parser=sentence_parse, compute_similarities=False)
    i1.ingest(docs)

    print "Beginning 4-gram ingestion at %s..." % datetime.now()
    c2 = Corpus(metadata=dict(docket=docket.id, agency=docket.agency, parser='4-gram'))
    i2 = DocumentIngester(c2, parser=ngram_parser(4), compute_similarities=True)
    i2.ingest(docs)
    
    print "Finished processing docket %s (corpuses %s and %s) at %s..." % (docket.id, c1.id, c2.id, datetime.now())


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-a', "--agency", dest="agency"),
        make_option('-d', "--docket", dest="docket"),
    )

    @transaction.commit_on_success
    def handle(self, **options):        
        if options.get('docket'):
            dockets = Docket.objects(id=options['docket'])
        elif options.get('agency'):
            dockets = Docket.objects(agency=option['agency'])
        else:
        	dockets = Docket.objects()

        print "Beginning loading %s dockets at %s..." % (len(dockets), datetime.now())

        for docket in dockets:
            ingest_docket(docket)

        print "Done."

