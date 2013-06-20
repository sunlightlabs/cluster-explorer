from optparse import make_option

from django.core.management.base import BaseCommand, CommandError

from analysis.utils import make_snapshot

class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            docket_id = args[0]
            outfile = args[1]
        except:
            raise CommandError("Please specify a docket and a filename.")

        make_snapshot(docket_id, outfile)