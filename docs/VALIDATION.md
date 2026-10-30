# Validation — 28 October 2026

Executed in the development environment:

- Generated 10,000 deterministic synthetic payments for 1 January 2026.
- Local SQLite ingestion: 10,000 input, 10,000 accepted, 0 rejected, 10,000 inserted.
- Repeated ingestion of the same file: skipped by its SHA-256 content hash.
- Two automated tests passed, covering deterministic generation, noncolliding IDs across days, exact duplicate handling, negative amount quarantine, conflicting existing ID quarantine, new-day ingestion, and reconciliation of Gold successful value/fees to Silver.
- Python modules and Databricks Python notebook passed syntax compilation.

Not executed: Databricks/Spark/Delta runtime, workspace permissions, SQL dashboard import, concurrent workloads, scale/performance, or recovery under injected partial failures. The notebook is an implementation prepared for validation, not a verified Databricks deployment.

## Workspace acceptance checks

1. Run 10,000 records and check Bronze=10,000, Silver=10,000, quarantine=0.
2. Rerun that file: Silver stays at 10,000; totals remain unchanged.
3. Add a second generated day: Silver becomes 20,000.
4. In a new file, include a negative amount and a changed existing ID; both must be quarantined.
5. Check sum of Gold attempted_count equals Silver count and successful_value_kobo/fee_revenue_kobo equal the corresponding Silver sums.
6. Confirm UTC session timezone and target catalog/schema before running dashboard SQL.

## Scope boundary

No claims of actual fraud detection accuracy, production reliability, real payment volume, performance gains, or financial impact. All fees and transaction distributions are illustrative. Customer identity history, changing payment statuses, scheduled jobs, and streaming remain future work.
