import os

from django.test import TestCase
from django.db import connection
from analysis.utils import execute_file
from analysis.corpus import Corpus

BASE_PATH = os.path.dirname(os.path.dirname(__file__))

class DBTestCase(TestCase):
    
    def setUp(self):
        self.cursor = connection.cursor()
        execute_file(self.cursor, os.path.join(BASE_PATH, 'tables.sql'))
        
        self.corpus = Corpus()
        
    def tearDown(self):
        execute_file(self.cursor, os.path.join(BASE_PATH, 'drop_tables.sql'))

class FastDBTestCase(DBTestCase):
    def setUp(self):
        super(FastDBTestCase, self).setUp()
        execute_file(self.cursor, os.path.join(BASE_PATH, 'drop_constraints.sql'))