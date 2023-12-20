{{
  config(
    materialized = "table"
  )
}}

WITH deposit_transactions_flat AS (
SELECT id,
    amount,
    centrekey,
    valuedate,
    type,
    creationdate,
    userkey,
    branchkey,
    parentaccountkey,
    encodedkey,
    bookingdate,
    currencycode,
    productid,
    transactionreferencenumber,
    transactionamounttype,
    corebankingid,
    serviceid,
    totalbalance,
    feesamount,
    overdraftinterestamount,
    overdraftfeesamount,
    fractionamount,
    technicaloverdraftamount,
    overdraftamount,
    interestamount,
    technicaloverdraftinterestamount,
    fundsamount,
    interestsettings,
    overdraftsettings,
    overdraftinterestsettings
FROM staging.stg_rtgs_deposit_transactions dt
INNER JOIN staging.stg_rtgs_deposit_tran__ns_transactiondetails dttdl ON dttdl._airbyte_stg_rtgs_dep__t_transactions_hashid = dt._airbyte_stg_rtgs_dep__t_transactions_hashid
INNER JOIN staging.stg_rtgs_deposit_tran__ransactiondescription dttdn ON dttdn._airbyte_stg_rtgs_dep__t_transactions_hashid = dt._airbyte_stg_rtgs_dep__t_transactions_hashid
INNER JOIN staging.stg_rtgs_deposit_tran__tions_accountbalances dtab ON dtab._airbyte_stg_rtgs_dep__t_transactions_hashid = dt._airbyte_stg_rtgs_dep__t_transactions_hashid
INNER JOIN staging.stg_rtgs_deposit_tran__tions_affectedamounts dtad ON dtad._airbyte_stg_rtgs_dep__t_transactions_hashid = dt._airbyte_stg_rtgs_dep__t_transactions_hashid
INNER JOIN staging.stg_rtgs_deposit_transactions_terms dtt ON dtt._airbyte_stg_rtgs_dep__t_transactions_hashid = dt._airbyte_stg_rtgs_dep__t_transactions_hashid
)

SELECT *
FROM deposit_transactions_flat


