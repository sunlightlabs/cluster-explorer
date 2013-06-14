from optparse import make_option

from django.core.management.base import BaseCommand, CommandError

from analysis.corpus import Corpus, get_corpora_by_metadata, get_dual_corpora_by_metadata
from analysis import bsims

import zipfile, cStringIO, os, json
from django.db import connection

class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            docket_id = args[0]
            outfile = args[1]
        except:
            raise CommandError("Please specify a docket and a filename.")

        c = connection.cursor()
        c.execute("select corpus_id, metadata->'parser' from corpora where metadata -> %s = %s", ["docket_id", docket_id])
        corpora = c.fetchall()
        
        if not corpora:
            raise CommandError("No corpus found for %s" % docket_id)
            
        ngram_corpora = [id for (id, parser) in corpora if parser=='4-gram']
        sentence_corpora = [id for (id, parser) in corpora if parser=='sentence']

        # open the export file
        zf = zipfile.ZipFile(outfile, 'w')

        # export the database stuff for each one
        buf = cStringIO.StringIO()
        for corpus_id in ngram_corpora + sentence_corpora:
            for table in ['corpora', 'documents', 'phrases', 'phrase_occurrences']:
                buf.truncate(0)
                c.copy_expert("copy (select * from %s where corpus_id = %s) to stdout with (format 'binary')" % (table, corpus_id), buf)
                zf.writestr(os.path.join("db", str(corpus_id), "%s.dump" % table), buf.getvalue(), zipfile.ZIP_DEFLATED)
        del buf

        # copy in the cluster files (force LZ4 for now)
        for corpus_id in ngram_corpora:
            reader = bsims.get_similarity_reader(corpus_id, force_data_type="lz4")
            for threshold, lz4files in reader.files_by_cutoff():
                for lz4file in lz4files:
                    zf.write(lz4file, os.path.join("sims", str(corpus_id), *(lz4file.split('/')[-2:])), zipfile.ZIP_STORED)

        # stash a list of the corpora so we can find them later
        zf.writestr("corpora", json.dumps({
            'ngram_corpora': ngram_corpora,
            'sentence_corpora': sentence_corpora
        }).encode('utf8'), zipfile.ZIP_DEFLATED)

        zf.close()