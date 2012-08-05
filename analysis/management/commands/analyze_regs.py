from datetime import datetime
from optparse import make_option

from mongoengine.queryset import Q

from django.db import transaction
from django.core.management.base import BaseCommand

from analysis.corpus import Corpus, get_corpora_by_metadata
from analysis.ingestion import DocumentIngester
from analysis.parser import ngram_parser, sentence_parse

from regs_models import Docket, Doc

def doc_text(doc):
    return "\n".join([doc.canonical_view().as_text()] + [a.canonical_view().as_text() for a in doc.attachments])[:10000]

def doc_metadata(doc):
	return {
		'document_id': doc.id,
		'title': doc.title,
		'agency_id': doc.agency,
		'docket_id': doc.docket_id,
		'type': doc.type,
        'submitter_name': " ".join([doc.details.get('First_Name', ''), doc.details.get('Last_Name', '')]),
        'submitter_organization': doc.details.get('Organization_Name', ''), 
		'created': unicode(doc.created),
		'last_seen': unicode(doc.last_seen),
        'ingested': unicode(datetime.now())
	}


# two check methods aren't used in command.
# can be run from shell before or after ingestion to test integrity

def check_pre_ingestion_counts(corpus_id, docket_id):
    mongo_analyzed = Doc.objects(docket_id=docket_id, in_cluster_db=True, deleted=False).count()
    mongo_unanalyzed = Doc.objects(docket_id=docket_id, in_cluster_db=False, deleted=False).count()
    mongo_deleted = Doc.objects(docket_id=docket_id, deleted=True).count()

    postgres_analyzed = Corpus(corpus_id).num_docs()

    print "MongoDB has %s analyzed documents, %s unanalyzed documents and %s deleted documents. Postgres has %s analyzed documents." % (mongo_analyzed, mongo_unanalyzed, mongo_deleted, postgres_analyzed)
    
    if mongo_analyzed != postgres_analyzed:
        print "WARNING: MongoDB and Postgres analyzed counts out of sync!"

def check_post_ingestion_counts(corpus_id, docket_id):
    mongo_analyzed = Doc.objects(docket_id=docket_id, in_cluster_db=True, deleted=False).count()
    mongo_unanalyzed = Doc.objects(docket_id=docket_id, in_cluster_db=False, deleted=False).count()
    mongo_deleted = Doc.objects(docket_id=docket_id, deleted=True).count()

    postgres_analyzed = Corpus(corpus_id).num_docs()

    print "MongoDB has %s analyzed documents, %s unanalyzed documents and %s deleted documents. Postgres has %s analyzed documents." % (mongo_analyzed, mongo_unanalyzed, mongo_deleted, postgres_analyzed)
    
    if mongo_analyzed != postgres_analyzed:
        print "WARNING: MongoDB and Postgres analyzed counts out of sync!"
    if mongo_unanalyzed != 0:
        print "WARNING: MongoDB still has unalayzed documents!"


def ingest_docket(docket):
    print "Loading docket %s at %s..." % (docket.id, datetime.now())

    deletions = Doc.objects(Q(docket_id=docket.id) & (Q(in_cluster_db=False) | Q(deleted=True))).scalar('id')

    insertions = [
        dict(text=doc_text(d), metadata=doc_metadata(d))
        for d in Doc.objects(docket_id=docket.id, deleted=False, in_cluster_db=False, type='public_submission')]

    
    print "Found %s documents for deletion or update, %s documents for insertion." % (len(deletions), len(insertions))

    if not insertions and not deletions:
        return

    with transaction.commit_on_success():
        ingest_single_parse(docket, deletions, insertions, 'sentence')
        ingest_single_parse(docket, deletions, insertions, '4-gram')

    print "Marking MongoDB documents as analyzed at %s..." % datetime.now()
    update_count = Doc.objects(id__in=[d['metadata']['document_id'] for d in insertions]) \
                      .update(safe_update=True, set__in_cluster_db=True)
    if update_count != len(insertions):
        print "ERROR: %s documents inserted into Postgres, but only %s documents marked as analyzed in MongoDB." % (len(insertions, update_count))


def ingest_single_parse(docket, deletions, insertions, parser):
    if parser not in ('sentence', '4-gram'):
        raise "Parser must be one of 'sentence' or '4-gram'. Got '%s'." % parser

    corpora = get_corpora_by_metadata('docket', docket.id)

    parsed_corpora = [c for c in corpora if c.metadata.get('parser') == parser]

    if len(parsed_corpora) == 0:
        c = Corpus(metadata=dict(docket_id=docket.id, agency_id=docket.agency, parser=parser))
        print "Created new corpus #%s for %s parse." % (c.id, parser)
    
    elif len(parsed_corpora) == 1:
        c = parsed_corpora[0]
        print "Updating existing corpus #%s for %s parse." % (c.id, parser)
        
        print "Deleting documents at %s..." % datetime.now()
        c.delete_by_metadata('document_id', deletions)
    
    else:
        raise "More than one sentence parse for docket %s found. Shouldn't happen--will need ot manually remove extra corpora." % docket.id
    
    print "Inserting documents at %s..." % datetime.now()
    if parser == 'sentence':
        i = DocumentIngester(c, parser=sentence_parse, compute_similarities=False)
    elif parser == '4-gram':
        i = DocumentIngester(c, parser=ngram_parser(4), compute_similarities=True)
    i.ingest(insertions)


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-a', "--agency", dest="agency"),
        make_option('-d', "--docket", dest="docket"),
    )

    @transaction.commit_manually
    def handle(self, **options):        
        if options.get('docket'):
            dockets = Docket.objects(id=options['docket'])
        elif options.get('agency'):
            dockets = Docket.objects(agency=options['agency'])
        else:
        	dockets = Docket.objects()

        print "Beginning loading %s dockets at %s..." % (len(dockets), datetime.now())

        for docket in dockets:
            ingest_docket(docket)

        print "Done."

