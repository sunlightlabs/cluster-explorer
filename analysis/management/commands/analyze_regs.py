from datetime import datetime
from optparse import make_option

from django.db import transaction
from django.core.management.base import BaseCommand

from analysis.corpus import Corpus, get_corpora_by_metadata, get_dual_corpora_by_metadata
from analysis.ingestion import DocumentIngester
from analysis.parser import ngram_parser, sentence_parse
from analysis import bsims

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


# can be run from shell before or after ingestion to test integrity
def print_stats(docket_id):
    print "MongoDB has\t%s in_cluster_db=True, deleted=False;\t%s in_cluster_db=False,deleted=False" % \
        (Doc.objects(docket_id=docket_id,in_cluster_db=True,deleted=False, type='public_submission').count(),
        Doc.objects(docket_id=docket_id,in_cluster_db=False,deleted=False, type='public_submission').count())
    print "\t\t%s in_cluster_db=True, deleted=True;\t%s in_cluster_db=False,deleted=True" % \
        (Doc.objects(docket_id=docket_id,in_cluster_db=True,deleted=True, type='public_submission').count(),
        Doc.objects(docket_id=docket_id,in_cluster_db=False,deleted=True, type='public_submission').count())


    for corpus in get_corpora_by_metadata('docket_id', docket_id):
            print "Corpus %s (%s) has %s documents." % (corpus.id, corpus.metadata, corpus.num_docs())


def ingest_docket(docket):
    print "Loading docket %s at %s..." % (docket.id, datetime.now())

    deletions = list(Doc.objects(docket_id=docket.id, deleted=True, in_cluster_db=True, type='public_submission').scalar('id'))

    insertions = [
        dict(text=doc_text(d), metadata=doc_metadata(d))
        for d in Doc.objects(docket_id=docket.id, deleted=False, in_cluster_db=False, type='public_submission')]

    
    print "Found %s documents for deletion, %s documents for insertion or update." % (len(deletions), len(insertions))

    if not insertions and not deletions:
        return

    with transaction.commit_on_success():
        ingest_single_parse(docket, deletions, insertions, 'sentence')
        ingest_single_parse(docket, deletions, insertions, '4-gram')

    print "Marking MongoDB documents as analyzed at %s..." % datetime.now()
    update_count = Doc.objects(id__in=[d['metadata']['document_id'] for d in insertions]) \
                      .update(safe_update=True, set__in_cluster_db=True)
    if update_count != len(insertions):
        print "ERROR: %s documents inserted into Postgres, but only %s documents marked as analyzed in MongoDB." % (len(insertions), update_count)
    update_count = Doc.objects(id__in=deletions) \
                      .update(safe_update=True, set__in_cluster_db=False)
    if update_count != len(deletions):
        print "ERROR: %s documents deleted in Postgres, but only %s documents marked as deleted in MongoDB." % (len(deletions), update_count)


def ingest_single_parse(docket, deletions, insertions, parser):
    if parser not in ('sentence', '4-gram'):
        raise "Parser must be one of 'sentence' or '4-gram'. Got '%s'." % parser

    corpora = get_corpora_by_metadata('docket_id', docket.id)

    parsed_corpora = [c for c in corpora if c.metadata.get('parser') == parser]

    if len(parsed_corpora) == 0:
        c = Corpus(metadata=dict(docket_id=docket.id, agency_id=docket.agency, parser=parser))
        print "Created new corpus #%s for %s parse." % (c.id, parser)
    
    elif len(parsed_corpora) == 1:
        c = parsed_corpora[0]
        print "Updating existing corpus #%s for %s parse." % (c.id, parser)
        
        print "Deleting documents at %s..." % datetime.now()
        c.delete_by_metadata('document_id', deletions + [d['metadata']['document_id'] for d in insertions])
    
    else:
        raise "More than one sentence parse for docket %s found. Shouldn't happen--will need ot manually remove extra corpora." % docket.id
    
    print "Inserting documents at %s..." % datetime.now()
    if parser == 'sentence':
        i = DocumentIngester(c, parser=sentence_parse, compute_similarities=False)
    elif parser == '4-gram':
        i = DocumentIngester(c, parser=ngram_parser(4), compute_similarities=True)
    i.ingest(insertions)

    print "Removing hierarchy, if cached, at %s..." % datetime.now()
    c.delete_hierarchy_cache()


def repair_missing_docket(docket):
    """Recreate any dockets that Mongo thinks are analyzed already but aren't in Postgres.

    Note that this is a very limited form or repair, corresponding to the particular
    situation in which some malformed dockets have been deleted from Postgres by
    hand, but not marked as such on the Mongo side. As other particular problems
    arise we may add different repair methods.
    """

    # only repair if MongoDB thinks that something should be in Postgres already
    if Doc.objects(docket_id=docket.id, in_cluster_db=True).count() == 0:
        return

    # does docket exist at all?
    corpora = get_corpora_by_metadata('docket_id', docket.id)
    
    if len(corpora) == 0:
        # neither parse exists, mark as unclustered in Mongo
        update_count = Doc.objects(docket_id=docket.id, in_cluster_db=True).update(safe_update=True, set__in_cluster_db=False)
        print "Docket %s missing in Postgres. Marked %s documents with in_cluster_db=False." % (docket.id, update_count)
        ingest_docket(docket)
    elif len(corpora) == 1 or len(corpora) > 2:
        # we have a single or multiple parses...that's something unexpected that we can't fix automatically
        raise "Found %s corpora for docket %s. Expected either 0 or 2 corpora. Must fix by hand." % (len(corpora), docket.id)

    # both parses exist, everything's fine

def repair_missing_sims(docket):
    """Repair the situation where a docket is correct in Mongo and Postgres,
    but the similarity directory is missing."""

    with transaction.commit_on_success():
        c = get_dual_corpora_by_metadata('docket_id', docket.id)
        if c and not bsims.exists(c.id):
                print "Docket %s (id=%s) missing similarities. Starting recomputation at %s..." % (docket.id, c.id, datetime.now())
                i = DocumentIngester(c)
                i.compute_similarities()


def delete_analysis(docket):
    with transaction.commit_on_success():
        c = get_dual_corpora_by_metadata('docket_id', docket.id)
        if c:
            c.delete_corpus()
            print "Deleted docket %s (id=%s)." % (docket.id, c.id)
        else:
            print "Attempted deletion of %s. Docket not found." % docket.id
        Doc.objects(docket_id=docket.id).update(set__in_cluster_db=False)

def process_docket(docket, options):
    with transaction.commit_manually():
        if options.get('repair'):
            repair_missing_docket(docket)
        elif options.get('delete'):
            delete_analysis(docket)
        elif options.get('repair_sims'):
            repair_missing_sims(docket)
        else:
            ingest_docket(docket)

class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-a', "--agency", dest="agency"),
        make_option('-d', "--docket", dest="docket"),
        make_option('-r', "--repair", dest='repair', action="store_true"),
        make_option("--delete", dest='delete', action='store_true'),
        make_option("--repair_sims", dest='repair_sims', action='store_true'),
        make_option('-F', "--fork", dest="fork", action="store_true")
    )

    def handle(self, **options):        
        if options.get('docket'):
            dockets = Docket.objects(id=options['docket'])
        elif options.get('agency'):
            dockets = Docket.objects(agency=options['agency'])
        else:
        	dockets = Docket.objects()

        print "Enumerating dockets..."
        docket_list = list(dockets.only('id', 'agency'))

        print "Beginning loading %s dockets at %s..." % (len(docket_list), datetime.now())

        if options['fork']:
            print "Using forking strategy..."
            import multiprocessing
            for docket in docket_list:
                p = multiprocessing.Process(target=process_docket, args=[docket, options])
                p.start()
                p.join()
        else:
            print "Using single-process strategy..."
            for docket in docket_list:
                process_docket(docket, options)            

        print "Done."


