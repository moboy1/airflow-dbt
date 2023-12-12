{{
  config(
    materialized = "table"
  )
}}

WITH journal_entries_flat AS (
SELECT accountkey,
    je.amount AS journal_entries_amount,
    notes,
    je.type AS journal_entries_type,
    productkey,
    je.creationdate AS journal_entries_creationdate,
    assignedbranchkey,
    userkey,
    transactionid,
    entryid,
    reversalentrykey,
    je.encodedkey AS journal_entries_encodedkey,
    bookingdate,
    producttype,
    jef.amount AS foreign_amount,
    jefa.rate AS foreign_amount_accountingrate,
    jefa.enddate AS foreign_amount_accountingrate_enddate,
    jefa.tocurrencycode AS foreign_amount_accountingrate_tocurrencycode,
    jefa.encodedkey AS foreign_amount_accountingrate_encodedkey,
    jefa.startdate AS foreign_amount_accountingrate_startdate,
    jefa.fromcurrencycode AS foreign_amount_accountingrate_fromcurrencycode,
    jefc.code AS foreign_amount_code,
    jefc.id AS foreign_amount_currency_id,
    jefc.currencycode AS foreign_amount_currency_code,
    jeg.migrationeventkey AS gl_account_migrationeventkey,
    jeg.lastmodifieddate AS gl_account_lastmodifieddate,
    jeg.usage AS gl_account_usage,
    jeg.glcode AS gl_account_glcode,
    jeg.description AS gl_account_description,
    jeg.type AS gl_account_type,
    jeg.creationdate AS gl_account_creationdate,
    jeg.allowmanualjournalentries AS gl_account_allowmanualjournalentries,
    jeg.balance AS gl_account_balance,
    jeg.name AS gl_account_name,
    jeg.encodedkey AS gl_account_encodedkey,
    jeg.striptrailingzeros AS gl_account_striptrailingzeros,
    jeg.activated AS gl_account_activated,
    jegc.code AS gl_account_code,
    jegc.id AS gl_account_currency_id,
    jegc.currencycode AS gl_account_currencycode
FROM staging.rtgs_journal_entries je
INNER JOIN staging.rtgs_journal_entries_foreignamount jef ON jef._airbyte_rtgs_journal_entries_hashid = je._airbyte_rtgs_journal_entries_hashid
INNER JOIN staging.rtgs_journal_entries___amount_accountingrate jefa ON jefa._airbyte_foreignamount_hashid = jef._airbyte_foreignamount_hashid
INNER JOIN staging.rtgs_journal_entries_foreignamount_currency jefc ON jefc._airbyte_foreignamount_hashid = jef._airbyte_foreignamount_hashid
INNER JOIN staging.rtgs_journal_entries_glaccount jeg ON jeg._airbyte_rtgs_journal_entries_hashid = je._airbyte_rtgs_journal_entries_hashid
INNER JOIN staging.rtgs_journal_entries_glaccount_currency jegc ON jegc._airbyte_glaccount_hashid = jeg._airbyte_glaccount_hashid
)

SELECT *
FROM journal_entries_flat


