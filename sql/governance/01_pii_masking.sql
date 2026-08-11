-- ===========================================================================
-- PII / PCI protection for FinGuard.
--
-- WHY THIS EXISTS
-- ---------------
-- This platform stores primary account numbers (PANs). Before this file,
-- `SELECT card_number FROM finguard.silver.transactions` returned
-- a full 16-digit PAN in clear text (4144 88** **** 9904, redacted here) to
-- SELECT on the table. There were no column masks, no row filters, no
-- classification tags, and the only grant on the catalog was
-- `account users BROWSE`.
--
-- For a card-fraud platform that is not a gap, it is a contradiction: the
-- system exists to protect cardholders and was exposing the one field the
-- card networks care most about.
--
-- WHY MASKING IN UNITY CATALOG RATHER THAN IN THE MODELS
-- -----------------------------------------------------
-- `dim_customer` already masks the PAN in SQL:
--
--     concat('****-****-****-', right(card_number, 4)) as card_number_masked
--
-- That is useful but it is not a control. It protects one derived table while
-- silver.transactions, silver.customers and gold.fraud_card_alert -- the
-- tables an analyst actually queries during a live investigation -- stayed
-- clear. A mask applied in a model protects that model. A mask applied in
-- Unity Catalog protects the column, for every reader, through every engine:
-- SQL warehouse, notebook, dbt, JDBC, Power BI. Anything that reads the table
-- gets the mask, including paths nobody thought about when writing the model.
--
-- THE PCI-DSS FRAME (stated precisely, not as a compliance claim)
-- --------------------------------------------------------------
-- PCI-DSS Requirement 3.3 says the PAN must be masked when displayed, with a
-- maximum of the first six and last four digits visible, and that only
-- personnel with a documented business need may see more. This file
-- implements the "last four" form and the "documented business need" as group
-- membership.
--
-- What this file does NOT do, stated plainly so nobody mistakes the scope:
-- masking is not encryption. Requirement 3.5 wants the PAN rendered
-- unreadable *at rest* -- tokenization, or strong crypto with managed keys.
-- The bytes here are still clear text on storage; the mask is an access-time
-- transformation. Anyone with direct access to the underlying cloud storage,
-- or with permission to read the Delta files outside Unity Catalog, still
-- sees the PAN. Real remediation is to tokenize at ingest so the PAN never
-- lands. That is a larger change to the producer and bronze layer; this file
-- is the access-control layer, and calling it "PCI compliant" would be
-- exactly the kind of unmeasured claim the rest of this project avoids.
--
-- DESIGN: TWO GROUPS
-- ------------------
--   finguard_pci_privileged   sees the full PAN. Membership is the
--                             "documented business need". Expected to be
--                             near-empty in normal operation.
--
--   finguard_fraud_analysts   sees ****-****-****-1234. Enough to confirm a
--                             card with a customer on the phone, useless to
--                             an attacker who exfiltrates the table.
--
-- Everyone else also gets the masked form. The mask defaults to *protecting*,
-- so a new user added tomorrow is safe without anyone remembering to
-- configure them. A mask that defaults to exposing is a mask that leaks the
-- first time someone is onboarded in a hurry.
--
-- PREREQUISITE (must exist before running this file):
--   The two account groups above. Create them in the account console under
--   User management -> Groups, or:
--     databricks account groups create --json '{"displayName":"finguard_pci_privileged"}'
--     databricks account groups create --json '{"displayName":"finguard_fraud_analysts"}'
--
--   The functions below reference the groups by name via
--   is_account_group_member(). That function returns FALSE for a group that
--   does not exist rather than erroring, which means a missing group fails
--   *closed* -- everyone sees the masked value. Safe, but verify the groups
--   exist or the privileged path will silently never work.
--
-- Run:  databricks sql -f sql/governance/01_pii_masking.sql
--   or paste into a SQL editor. Every statement is idempotent.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. Masking functions
--
-- Functions live in a dedicated schema rather than beside the data. A mask is
-- a security control, and keeping controls in `finguard.security` means the
-- permission to ALTER a mask is separable from the permission to read the
-- table it protects. Someone who can edit the mask can disable it.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS finguard.security
    COMMENT 'Masking functions and access-control objects. Not a data schema.';


-- PAN mask. Returns the last four digits in the standard display form.
--
-- RETURN TYPE MUST MATCH THE COLUMN TYPE. card_number is STRING throughout
-- this platform, so this returns STRING. A mask whose return type differs
-- from the column type is rejected at ALTER time -- which is the good case.
-- The bad case is a mask that returns a *wider* compatible type and silently
-- changes downstream behaviour.
CREATE OR REPLACE FUNCTION finguard.security.mask_pan(pan STRING)
COMMENT 'PCI-DSS 3.3 display masking. Last four digits only, except for members of finguard_pci_privileged.'
RETURN
    CASE
        -- Privileged callers see the real value. This branch is the entire
        -- reason the function is not just `concat('****-', right(pan,4))`.
        WHEN is_account_group_member('finguard_pci_privileged') THEN pan

        -- NULL in, NULL out. Without this the mask turns a NULL PAN into the
        -- string '****-****-****-' with nothing after it, which reads like a
        -- real (empty) card rather than absent data, and breaks IS NULL
        -- checks downstream.
        WHEN pan IS NULL THEN NULL

        -- Defensive: a value too short to have a last-four is fully masked
        -- rather than partially revealed. A 4-digit value would otherwise be
        -- returned in its entirety by right(pan, 4).
        WHEN length(pan) < 8 THEN '****'

        ELSE concat('****-****-****-', right(pan, 4))
    END;



-- Email mask. Keeps the first character and the domain.
--
-- Preserving the domain is deliberate: it is operationally useful (spotting
-- that alerts cluster on one corporate domain) and is not itself the
-- sensitive part. The local part is what identifies the individual.
CREATE OR REPLACE FUNCTION finguard.security.mask_email(email STRING)
COMMENT 'Partial email masking. First character and domain retained for triage.'
RETURN
    CASE
        WHEN is_account_group_member('finguard_pci_privileged') THEN email
        WHEN email IS NULL THEN NULL
        -- No '@' means this is not an address; do not attempt to split it,
        -- just withhold it. substring_index returns the whole string when the
        -- delimiter is absent, which would leak the value verbatim.
        WHEN locate('@', email) = 0 THEN '***'
        ELSE concat(
            left(email, 1), '***@',
            substring_index(email, '@', -1)
        )
    END;



-- ---------------------------------------------------------------------------
-- 2. Apply the masks
--
-- SET MASK is idempotent in effect but not in syntax: re-applying a mask to a
-- column that already has one raises an error. Each ALTER is therefore
-- preceded by a DROP that is safe to run when no mask is present.
--
-- Applied to every table carrying the column, not just the "important" one.
-- Masking silver but not gold would mean the alert table -- the one an
-- analyst opens first when responding to a fraud alert -- is the leak.
-- ---------------------------------------------------------------------------

-- silver.transactions: the highest-volume PAN store (4,413 rows).
ALTER TABLE finguard.silver.transactions
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.silver.transactions
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

-- silver.customers: PAN plus email on the customer master.
ALTER TABLE finguard.silver.customers
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.silver.customers
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

ALTER TABLE finguard.silver.customers
    ALTER COLUMN email DROP MASK;
ALTER TABLE finguard.silver.customers
    ALTER COLUMN email SET MASK finguard.security.mask_email;

-- gold.fraud_card_alert: the investigation surface. Carries both.
ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN customer_email DROP MASK;
ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN customer_email SET MASK finguard.security.mask_email;

-- silver.transactions_quarantine: rejected rows are still real cardholder
-- data. Quarantine is the easiest place to forget, and the easiest place to
-- over-grant, because it looks like "bad data" rather than customer data.
--
-- NOTE ON raw_payload: that column holds the verbatim Kafka message, which
-- contains the PAN inside a JSON string. A column mask cannot reach inside
-- it. Masking card_number here while raw_payload sits beside it in clear text
-- would be security theatre, so the raw column is masked wholesale for
-- non-privileged callers -- see 03_pii_verification.sql, which asserts this
-- specifically. The real fix is tokenization at ingest.
ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;


-- Raw payload: withhold entirely from non-privileged callers.
CREATE OR REPLACE FUNCTION finguard.security.mask_raw_payload(payload STRING)
COMMENT 'Raw Kafka payloads embed the PAN in JSON, where a column mask cannot reach, so the whole value is withheld.'
RETURN
    CASE
        WHEN is_account_group_member('finguard_pci_privileged') THEN payload
        WHEN payload IS NULL THEN NULL
        ELSE '[REDACTED - contains unparsed PAN, privileged access required]'
    END;


ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN raw_payload DROP MASK;
ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN raw_payload SET MASK finguard.security.mask_raw_payload;


-- ---------------------------------------------------------------------------
-- 3. Classification tags
--
-- Masks control access. Tags make classification *queryable* -- the
-- difference between "Taha knows which columns are sensitive" and "the
-- platform can answer which columns are sensitive". Without tags, the only
-- way to audit PII coverage is to read every DDL by hand and hope.
--
-- The payoff query is in 03_pii_verification.sql: find every column tagged
-- pii/pci that does NOT have a mask applied. That catches the case this whole
-- file exists to fix -- a new table added next month carrying card_number
-- that nobody remembered to mask.
-- ---------------------------------------------------------------------------

ALTER TABLE finguard.silver.transactions
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');

ALTER TABLE finguard.silver.customers
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');
ALTER TABLE finguard.silver.customers
    ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity' = 'confidential');

ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');
ALTER TABLE finguard.gold.fraud_card_alert
    ALTER COLUMN customer_email SET TAGS ('pii' = 'true', 'sensitivity' = 'confidential');

ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');
ALTER TABLE finguard.silver.transactions_quarantine
    ALTER COLUMN raw_payload SET TAGS ('pii' = 'true', 'pci' = 'pan-embedded', 'sensitivity' = 'restricted');

-- ---------------------------------------------------------------------------
-- 2b. The tables the first pass missed.
--
-- These were NOT in the original plan. They were found by CHECK 4 in
-- 03_pii_verification.sql -- the query that looks for PAN-shaped columns
-- regardless of whether anyone classified them -- immediately after the first
-- six masks were applied and verified as working.
--
-- This is the entire argument for writing CHECK 4 rather than stopping at
-- "the masks I applied are attached". The first pass covered the tables that
-- came to mind: silver, gold, quarantine. It missed:
--
--   bronze.customers            CDC landing table. Held 5026 54** **** 1551 in
--                               clear. Missed because the threat model was
--                               "transactions carry PANs" -- but the customer
--                               master carries the card on file, and CDC
--                               writes it to bronze before silver ever runs.
--
--   snapshots.customers_snapshot  dbt's SCD2 snapshot. Held 5212 34** **** 1234
--                               in clear. Missed because it is written by dbt
--                               rather than by the pipeline, so it sat
--                               outside the mental model of "the tables I
--                               own". It is also the worst one to miss: a
--                               snapshot retains every historical version of
--                               a customer row forever, so it accumulates
--                               PANs that no longer exist anywhere else.
--
--   silver.customers_quarantine  Currently empty, so it leaks nothing today.
--                               Masked anyway -- an empty table is a table
--                               that has not been populated *yet*, and the
--                               moment a customer row fails validation it
--                               will hold a PAN. Masking on the basis of
--                               current contents rather than schema is how
--                               controls rot.
--
-- The marts (stg_customers, stg_transactions, dim_customer) were checked and
-- need nothing: stg_* are VIEWS over masked silver tables, so they inherit
-- the mask, and dim_customer's card_number_masked is already the derived
-- last-four. Verified by reading actual values, not by reasoning about it.
-- ---------------------------------------------------------------------------

ALTER TABLE finguard.bronze.customers
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.bronze.customers
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

ALTER TABLE finguard.silver.customers_quarantine
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.silver.customers_quarantine
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

-- NOTE: snapshots.customers_snapshot is written by `dbt snapshot`, which
-- writes via MERGE into an existing table rather than recreating it, so the
-- mask survives normal runs. It would NOT survive `dbt snapshot --full-refresh`.
-- Same class of trap as a Lakeflow full refresh -- see CHECK 6.
ALTER TABLE finguard.snapshots.customers_snapshot
    ALTER COLUMN card_number DROP MASK;
ALTER TABLE finguard.snapshots.customers_snapshot
    ALTER COLUMN card_number SET MASK finguard.security.mask_pan;

ALTER TABLE finguard.bronze.customers
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');
ALTER TABLE finguard.silver.customers_quarantine
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');
ALTER TABLE finguard.snapshots.customers_snapshot
    ALTER COLUMN card_number SET TAGS ('pii' = 'true', 'pci' = 'pan', 'sensitivity' = 'restricted');


-- Table-level tags: the unit a data steward searches on.
ALTER TABLE finguard.silver.transactions
    SET TAGS ('data_domain' = 'payments', 'contains_pci' = 'true');
ALTER TABLE finguard.silver.customers
    SET TAGS ('data_domain' = 'customer', 'contains_pci' = 'true');
ALTER TABLE finguard.gold.fraud_card_alert
    SET TAGS ('data_domain' = 'fraud', 'contains_pci' = 'true');
