{{
  config(
    materialized = "table"
  )
}}

WITH journal_entries_flat AS (
SELECT accountkey,
    je.amount AS journal_entries_amount,
    je.type AS journal_entries_type,
    productkey,
    je.creationdate AS journal_entries_creationdate,
    assignedbranchkey,
    userkey,
    transactionid,
    entryid,
    je.encodedkey AS journal_entries_encodedkey,
    bookingdate,
    producttype,
    jeg.lastmodifieddate AS gl_account_lastmodifieddate,
    jeg.usage AS gl_account_usage,
    jeg.glcode AS gl_account_glcode,
    jeg.description AS gl_account_description,
    jeg.type AS gl_account_type,
    jeg.creationdate AS gl_account_creationdate,
    jeg.allowmanualjournalentries AS gl_account_allowmanualjournalentries,
    jeg.name AS gl_account_name,
    jeg.encodedkey AS gl_account_encodedkey,
    jeg.striptrailingzeros AS gl_account_striptrailingzeros,
    jeg.activated AS gl_account_activated,
    jegc.code AS gl_account_code,
    jegc.currencycode AS gl_account_currencycode
FROM staging.stg_rtgs_journal_entries je
INNER JOIN staging.stg_rtgs_journal_entries_glaccount jeg ON jeg._airbyte_stg_rtgs_journal_entries_hashid = je._airbyte_stg_rtgs_journal_entries_hashid
INNER JOIN staging.stg_rtgs_journal_entries_glaccount_currency jegc ON jegc._airbyte_glaccount_hashid = jeg._airbyte_glaccount_hashid
)

SELECT *
FROM journal_entries_flat


