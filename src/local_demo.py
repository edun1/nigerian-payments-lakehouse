"""SQLite reference demo. Databricks implementation is in notebooks/."""
import argparse
import csv
import hashlib
import json
from contextlib import closing
from datetime import datetime
from pathlib import Path
import sqlite3
from generate import CHANNELS, STATES, FIELDS

def reason(r):
    if not r.get('transaction_id') or not r.get('customer_id'):
        return 'MISSING_ID'
    try:
        datetime.fromisoformat(r['event_time'])
        amount, fee = int(r['amount_kobo']), int(r['fee_kobo'])
    except (ValueError, KeyError, TypeError):
        return 'INVALID_TYPE'
    if amount <= 0 or fee < 0 or fee > amount:
        return 'INVALID_MONEY'
    if r['currency'] != 'NGN' or r['channel'] not in CHANNELS or r['state'] not in STATES:
        return 'INVALID_CATEGORY'
    if r['status'] not in ['SUCCESS','FAILED','PENDING']:
        return 'INVALID_STATUS'
    if r['status'] != 'SUCCESS' and fee != 0:
        return 'INVALID_FEE'
    return None

def run(source, database):
    content = Path(source).read_bytes()
    batch_id = hashlib.sha256(content).hexdigest()
    with closing(sqlite3.connect(database)) as con, con:
        con.executescript('''
        CREATE TABLE IF NOT EXISTS bronze(batch_id TEXT, row_number INTEGER, payload TEXT,
          PRIMARY KEY(batch_id,row_number));
        CREATE TABLE IF NOT EXISTS quarantine(batch_id TEXT, row_number INTEGER, reason TEXT,
          payload TEXT, PRIMARY KEY(batch_id,row_number));
        CREATE TABLE IF NOT EXISTS silver(transaction_id TEXT PRIMARY KEY, customer_id TEXT,
          event_time TEXT, amount_kobo INTEGER, fee_kobo INTEGER, currency TEXT,
          channel TEXT, status TEXT, state TEXT);
        CREATE TABLE IF NOT EXISTS audit(batch_id TEXT PRIMARY KEY, input_rows INTEGER,
          accepted_rows INTEGER, rejected_rows INTEGER, inserted_rows INTEGER);
        ''')
        existing = con.execute('SELECT * FROM audit WHERE batch_id=?', (batch_id,)).fetchone()
        if existing:
            return {'batch_id':batch_id, 'skipped':True}
        reader = csv.DictReader(content.decode('utf-8').splitlines())
        if reader.fieldnames != FIELDS:
            raise ValueError('Unexpected CSV header')
        rows = list(reader)
        if any(None in row or any(v is None for v in row.values()) for row in rows):
            raise ValueError('Malformed CSV row')
        accepted = rejected = inserted = 0
        # Transaction ID is immutable: exact repeats are accepted no-ops; conflicts quarantined.
        for i, r in enumerate(rows):
            payload = json.dumps(r, sort_keys=True)
            con.execute('INSERT INTO bronze VALUES(?,?,?)', (batch_id,i,payload))
            error = reason(r)
            values = None
            if not error:
                values = (r['transaction_id'],r['customer_id'],r['event_time'],int(r['amount_kobo']),
                          int(r['fee_kobo']),r['currency'],r['channel'],r['status'],r['state'])
                old = con.execute('SELECT * FROM silver WHERE transaction_id=?', (values[0],)).fetchone()
                if old and old != values:
                    error = 'CONFLICTING_ID'
            if error:
                rejected += 1
                con.execute('INSERT INTO quarantine VALUES(?,?,?,?)', (batch_id,i,error,payload))
            else:
                accepted += 1
                inserted += con.execute('INSERT OR IGNORE INTO silver VALUES(?,?,?,?,?,?,?,?,?)', values).rowcount
        assert len(rows) == accepted + rejected, 'Row reconciliation failed'
        con.execute('INSERT INTO audit VALUES(?,?,?,?,?)', (batch_id,len(rows),accepted,rejected,inserted))
        con.execute('DROP TABLE IF EXISTS gold_daily')
        con.execute('''CREATE TABLE gold_daily AS SELECT substr(event_time,1,10) AS payment_date,
          channel, state, COUNT(*) AS attempted_count,
          SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS successful_count,
          SUM(CASE WHEN status='SUCCESS' THEN amount_kobo ELSE 0 END) AS successful_value_kobo,
          SUM(fee_kobo) AS fee_revenue_kobo FROM silver GROUP BY 1,2,3''')
        assert con.execute('SELECT COALESCE(SUM(attempted_count),0) FROM gold_daily').fetchone()[0] == con.execute('SELECT COUNT(*) FROM silver').fetchone()[0]
        return dict(batch_id=batch_id, input_rows=len(rows), accepted_rows=accepted,
                    rejected_rows=rejected, inserted_rows=inserted)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('--database', default='payments.db')
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.database), indent=2))
