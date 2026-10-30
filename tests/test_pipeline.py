import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
import sqlite3
import tempfile
import unittest
from contextlib import closing
from generate import generate, write_csv
from local_demo import run

class PipelineTests(unittest.TestCase):
    def test_seed_and_days(self):
        self.assertEqual(generate(rows=30), generate(rows=30))
        a = {x['transaction_id'] for x in generate(rows=30)}
        b = {x['transaction_id'] for x in generate('2026-01-02',30)}
        self.assertFalse(a & b)

    def test_replay_reject_conflict_and_reconciliation(self):
        with tempfile.TemporaryDirectory() as d:
            source, db = Path(d)/'batch.csv', str(Path(d)/'demo.db')
            original = generate(rows=100)
            invalid = dict(original[0], transaction_id='BAD', amount_kobo=-1)
            write_csv(source, original+[invalid,original[0]])
            result = run(source, db)
            self.assertEqual((result['inserted_rows'],result['rejected_rows']), (100,1))
            self.assertTrue(run(source, db)['skipped'])
            conflict = dict(original[0], amount_kobo=999999999)
            write_csv(source, [conflict]+generate('2026-01-02',20))
            self.assertEqual(run(source, db)['rejected_rows'],1)
            with closing(sqlite3.connect(db)) as c, c:
                self.assertEqual(c.execute('SELECT COUNT(*) FROM silver').fetchone()[0],120)
                silver = c.execute("SELECT SUM(amount_kobo), SUM(fee_kobo) FROM silver WHERE status='SUCCESS'").fetchone()
                gold = c.execute('SELECT SUM(successful_value_kobo), SUM(fee_revenue_kobo) FROM gold_daily').fetchone()
                self.assertEqual(silver,gold)
                self.assertEqual(c.execute('SELECT COUNT(*) FROM quarantine').fetchone()[0],2)

if __name__ == '__main__':
    unittest.main()
