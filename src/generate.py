"""Deterministic synthetic payments; no real people or bank details."""
import argparse
import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import random

FIELDS = ['transaction_id','customer_id','event_time','amount_kobo','fee_kobo','currency','channel','status','state']
CHANNELS = ['BANK_TRANSFER','POS','CARD','USSD']
STATES = ['Lagos','Delta','Abuja FCT','Kano','Rivers','Oyo']

def generate(day='2026-01-01', rows=10000, seed=42):
    if rows < 1:
        raise ValueError('rows must be positive')
    start = datetime.combine(date.fromisoformat(day), datetime.min.time())
    rng = random.Random(f'{seed}:{day}')
    records = []
    for i in range(rows):
        amount = rng.randint(10000, 50000000)
        status = rng.choices(['SUCCESS','FAILED','PENDING'], weights=[92,5,3])[0]
        records.append(dict(zip(FIELDS, [
            f'TXN-{day}-{i:08d}', f'CUST-{rng.randint(1,5000):05d}',
            (start+timedelta(seconds=rng.randrange(86400))).isoformat(),
            amount, min(10000, amount//100) if status == 'SUCCESS' else 0,
            'NGN', rng.choice(CHANNELS), status, rng.choice(STATES)])))
    return records

def write_csv(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--day', default='2026-01-01')
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--output', default='data/2026-01-01.csv')
    args = parser.parse_args()
    write_csv(args.output, generate(args.day, args.rows))
    print(f'Created {args.rows:,} synthetic payments: {args.output}')
