# Databricks notebook source
# MAGIC %md
# MAGIC # Nigerian Payments Lakehouse — batch v1
# MAGIC Upload ONE generated CSV to a Unity Catalog Volume. Run this notebook serially.
# MAGIC Existing catalog, schema, volume and read/write permissions are prerequisites.

# COMMAND ----------
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema', 'payments_demo')
dbutils.widgets.text('source_path', '/Volumes/workspace/default/payments_demo/2026-01-01.csv')
import re
from pyspark.sql import functions as F
from delta.tables import DeltaTable
spark.conf.set('spark.sql.session.timeZone', 'UTC')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
source = dbutils.widgets.get('source_path')
assert all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', x) for x in [catalog,schema])
assert source.startswith('/Volumes/') and source.endswith('.csv'), 'Provide one CSV file in a Volume'
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
prefix = f'`{catalog}`.`{schema}`.'
# Hash bytes: a repeated file has the same batch identity regardless of filename.
files = spark.read.format('binaryFile').load(source)
assert files.count() == 1, 'One file per run'
batch_id = files.select(F.sha2('content',256).alias('id')).first()['id']
fields = ['transaction_id','customer_id','event_time','amount_kobo','fee_kobo','currency','channel','status','state']
raw = (spark.read.option('header',True).option('mode','FAILFAST')
       .option('inferSchema',False).csv(source))
assert raw.columns == fields, f'Unexpected columns: {raw.columns}'
raw = (raw.withColumn('batch_id',F.lit(batch_id))
       .withColumn('source_path',F.lit(source))
       .withColumn('ingested_at',F.current_timestamp()))
bronze_name = prefix+'bronze_payments'
if not spark.catalog.tableExists(bronze_name):
    raw.limit(0).write.format('delta').saveAsTable(bronze_name)
# Replace only this batch, preserving other batches and original field strings.
(raw.write.format('delta').mode('overwrite')
 .option('replaceWhere',f"batch_id = '{batch_id}'").saveAsTable(bronze_name))

# COMMAND ----------
bronze = spark.table(bronze_name).filter(F.col('batch_id') == batch_id)
# Require integer strings: reject fractional money instead of rounding it.
integer_money = (F.col('amount_kobo').rlike(r'^\d+$') & F.col('fee_kobo').rlike(r'^\d+$'))
# Attach raw values with a stable payload hash before casting, avoiding joins by nonunique ID.
bronze = bronze.withColumn('raw_payload', F.to_json(F.struct(*fields)))
typed = (bronze.withColumn('integer_money',integer_money)
 .withColumn('event_time',F.expr('try_cast(event_time AS TIMESTAMP)'))
 .withColumn('amount_kobo',F.expr('try_cast(amount_kobo AS BIGINT)'))
 .withColumn('fee_kobo',F.expr('try_cast(fee_kobo AS BIGINT)')))
valid = (
 F.col('transaction_id').isNotNull() & (F.length('transaction_id') > 0) &
 F.col('customer_id').isNotNull() & (F.length('customer_id') > 0) &
 F.col('event_time').isNotNull() & F.col('integer_money') &
 (F.col('amount_kobo') > 0) & (F.col('fee_kobo') >= 0) &
 (F.col('fee_kobo') <= F.col('amount_kobo')) &
 (F.col('currency') == 'NGN') &
 F.col('channel').isin('BANK_TRANSFER','POS','CARD','USSD') &
 F.col('status').isin('SUCCESS','FAILED','PENDING') &
 F.col('state').isin('Lagos','Delta','Abuja FCT','Kano','Rivers','Oyo') &
 ((F.col('status') == 'SUCCESS') | (F.col('fee_kobo') == 0)))
checked = typed.withColumn('valid',F.coalesce(valid,F.lit(False)))
bad = checked.filter(~F.col('valid')).withColumn('reason',F.lit('INVALID_RECORD'))
candidates = checked.filter('valid').withColumn('record_hash',F.sha2(F.to_json(F.struct(*fields)),256))
# An ID with different valid payloads in ONE batch is ambiguous: quarantine all its rows.
conflicting = (candidates.groupBy('transaction_id').agg(F.countDistinct('record_hash').alias('n'))
               .filter('n > 1').select('transaction_id'))
bad_conflicts = candidates.join(conflicting,'transaction_id').withColumn('reason',F.lit('CONFLICTING_ID'))
candidates = candidates.join(conflicting,'transaction_id','left_anti')
silver_name = prefix+'silver_payments'
if not spark.catalog.tableExists(silver_name):
    candidates.select(*fields,'record_hash','batch_id').limit(0).write.format('delta').saveAsTable(silver_name)
existing = spark.table(silver_name).select('transaction_id',F.col('record_hash').alias('existing_hash'))
compared = candidates.join(existing,'transaction_id','left')
conflict_existing = compared.filter('existing_hash IS NOT NULL AND record_hash <> existing_hash')
bad_existing = conflict_existing.withColumn('reason',F.lit('CONFLICTING_ID'))
accepted = compared.filter('existing_hash IS NULL OR record_hash = existing_hash')
reject_columns = ['batch_id','source_path','raw_payload','reason']
rejected = (bad.select(*reject_columns).unionByName(bad_conflicts.select(*reject_columns))
            .unionByName(bad_existing.select(*reject_columns)))
quarantine_name = prefix+'quarantine_payments'
if not spark.catalog.tableExists(quarantine_name):
    rejected.limit(0).write.format('delta').saveAsTable(quarantine_name)
(rejected.write.format('delta').mode('overwrite')
 .option('replaceWhere',f"batch_id = '{batch_id}'").saveAsTable(quarantine_name))
input_count, accepted_count, rejected_count = bronze.count(), accepted.count(), rejected.count()
assert input_count == accepted_count + rejected_count, 'Batch row reconciliation failed'
# Materialize counts before MERGE; inserting changes the target used in conflict comparison.
updates = accepted.select(*fields,'record_hash','batch_id').dropDuplicates(['transaction_id'])
(DeltaTable.forName(spark,silver_name).alias('t').merge(updates.alias('s'),
 't.transaction_id = s.transaction_id').whenNotMatchedInsertAll().execute())

# COMMAND ----------
silver = spark.table(silver_name)
assert silver.count() == silver.select('transaction_id').distinct().count(), 'Duplicate IDs'
gold = (silver.withColumn('payment_date',F.to_date('event_time'))
 .groupBy('payment_date','channel','state').agg(
 F.count('*').alias('attempted_count'),
 F.sum(F.when(F.col('status')=='SUCCESS',1).otherwise(0)).alias('successful_count'),
 F.sum(F.when(F.col('status')=='SUCCESS',F.col('amount_kobo')).otherwise(0)).alias('successful_value_kobo'),
 F.sum('fee_kobo').alias('fee_revenue_kobo')))
# Publish Gold only after totals reconcile. Money remains integer kobo in storage.
gold_totals = gold.agg(F.sum('attempted_count'),F.sum('successful_value_kobo'),F.sum('fee_revenue_kobo')).first()
expected = silver.agg(F.count('*'),F.sum(F.when(F.col('status')=='SUCCESS',F.col('amount_kobo')).otherwise(0)),F.sum('fee_kobo')).first()
assert tuple(x or 0 for x in gold_totals) == tuple(x or 0 for x in expected), 'Gold totals mismatch'
gold.write.format('delta').mode('overwrite').saveAsTable(prefix+'gold_daily_payments')
audit = spark.createDataFrame([(batch_id,input_count,accepted_count,rejected_count,'PASS')],
 'batch_id STRING, input_rows LONG, accepted_rows LONG, rejected_rows LONG, reconciliation_status STRING')
audit_name = prefix+'batch_quality'
if not spark.catalog.tableExists(audit_name):
    audit.limit(0).write.format('delta').saveAsTable(audit_name)
(DeltaTable.forName(spark,audit_name).alias('t').merge(audit.alias('s'),'t.batch_id = s.batch_id')
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
display(audit)
display(gold.orderBy('payment_date','channel','state'))
