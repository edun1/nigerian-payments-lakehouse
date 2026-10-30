# Nigerian Payments Lakehouse

A portfolio project by Blessing Atigbolaye Edun: synthetic Nigerian payments transformed into auditable daily metrics. Version 1 covers batch ingestion, validation, immutable transaction IDs, quarantine, and financial reconciliation.

## Business questions

- How much NGN payment value was successfully processed?
- What percentage of attempted payments succeeded?
- How much synthetic fee revenue was earned?
- Which channels and states account for activity?
- Which records were rejected and why?

Transaction value is not revenue. Fees are illustrative assumptions, not actual provider tariffs. All records are synthetic. This is an educational system, not a fraud detection or regulated reporting product.

## Architecture

Generated CSV → Bronze raw records → Silver valid unique payments → Gold daily metrics.
Invalid and conflicting records go to quarantine; audit counts reconcile inputs to accepted plus rejected rows. Storage uses integer **kobo** to avoid binary floating point rounding. NGN display values divide by 100.

## Start locally — no dependencies

Requires Python 3.10+. Extract the project and open a terminal in its root:

```bash
python src/generate.py --day 2026-01-01 --rows 10000 --output data/2026-01-01.csv
python src/local_demo.py data/2026-01-01.csv
python src/local_demo.py data/2026-01-01.csv
python -m unittest discover -s tests -v
```

The second ingestion reports `skipped: true`. The local reference writes `payments.db` using SQLite; it does not emulate Spark performance or Delta transactions. Inspect its `bronze`, `silver`, `quarantine`, `audit`, and `gold_daily` tables with any SQLite client.

Add a new day:

```bash
python src/generate.py --day 2026-01-02 --rows 10000 --output data/2026-01-02.csv
python src/local_demo.py data/2026-01-02.csv
```

## Run in Databricks

1. Generate the CSV locally using the first command above.
2. In your Databricks workspace, identify an accessible Unity Catalog catalog and schema. Create or use a Volume and upload the CSV. No cloud account or paid compute is provisioned by this project.
3. Import `notebooks/01_payments_pipeline.py` as a Python notebook. Attach compute with PySpark, Delta Lake, and Unity Catalog Volume support.
4. Set widgets: `catalog`, `schema` (target tables), and `source_path` (the single uploaded CSV under `/Volumes/<catalog>/<schema>/<volume>/...`). The Volume can be in a different schema from the target tables.
5. Run all cells. The principal needs source Volume read access, target catalog/schema access, schema creation permission if needed, and table create/read/write privileges.
6. Run again and verify Silver row count and Gold totals remain unchanged. Process a second day's file and verify growth by its unique accepted payments.
7. Import `notebooks/02_analytics.sql`, update its catalog/schema, and run the queries. Use the results to build dashboard charts: daily successful value, success rate, channel value, fee revenue, rejected-record counts.

Databricks execution is pending workspace validation. No Databricks cluster, account, credentials, or live payment data are bundled.

## Data contract

| Field | Meaning |
|---|---|
| transaction_id | Immutable event ID, unique across generated days |
| customer_id | Synthetic identifier; no customer dimension yet |
| event_time | ISO timestamp, interpreted as UTC |
| amount_kobo | Positive integer minor units |
| fee_kobo | Nonnegative integer; zero for unsuccessful payments; no greater than amount |
| currency | NGN |
| channel | BANK_TRANSFER, POS, CARD, USSD |
| status | SUCCESS, FAILED, PENDING |
| state | Lagos, Delta, Abuja FCT, Kano, Rivers, Oyo |

These are immutable payment events: a pending payment is not updated to success in v1. Status transitions require a future versioned-event model. A high-value payment alone does not imply fraud.

## Duplicate and failure behavior

- Exact replays do not create duplicate Silver payments.
- Different payloads for an existing transaction ID are quarantined, preserving the existing record.
- Databricks quarantines all conflicting valid versions within one batch. The sequential SQLite reference keeps the first valid version and rejects subsequent conflicts. This difference is intentional and must be considered when comparing pathological batches.
- Bronze retains raw string values and provenance. Databricks expects well-formed CSV with the exact header; structurally malformed files fail ingestion instead of being silently dropped.
- Databricks replaces Bronze/quarantine for the same file hash on replay; Silver uses insert-only Delta MERGE. Gold is rebuilt from all Silver data for this small demonstration.
- Run one batch at a time. Multi-table writes are not one transaction; rerun the same batch after a failed run to recover. Consumers should treat a successful completed notebook as the publication signal. A production version needs stronger publication/version controls.
- Rejected rows are an audited outcome, not a claim that every row is valid. Critical reconciliation and uniqueness failures raise exceptions before Gold publication.

## Validation and next work

See `docs/VALIDATION.md` for executed checks and limitations. Next milestones: validate in Databricks, configure job orchestration, add customer dimensions/SCD2 and monitoring, then integrate streaming and evaluated risk rules.

## Attribution

Conceptual inspiration: https://github.com/FathimaNafra/finance-databricks-data-engineering-project by Fathima Nafra. This starter is newly written code; no source notebooks, screenshots, or dataset files from that repository are included. No license claim is made about that repository.
