-- ===========================================================================
-- Access model for FinGuard.
--
-- BEFORE THIS FILE
-- ----------------
--     SHOW GRANTS ON CATALOG finguard;
--     -> account users  BROWSE  CATALOG  finguard
--
-- That was the entire access model. BROWSE lets a principal see that objects
-- exist without reading them, so the catalog was not wide open -- but nothing
-- described who *should* read what, and any grant made later would have been
-- clicked into the workspace and invisible to review. That is the same
-- problem the Asset Bundle solved for pipelines and create_alerts.py solved
-- for alerts: state that matters, living outside git.
--
-- WHY GROUPS AND NOT USERS
-- ------------------------
-- Every grant here targets a group. Granting to a named user means access
-- leaves with the person and has to be reconstructed by whoever inherits the
-- work -- and in practice it never is, so the grant is either lost or the
-- account is kept alive. Groups make membership the thing that changes and
-- permission the thing that stays.
--
-- This is also what makes the masking in 01_pii_masking.sql work at all: the
-- masks branch on is_account_group_member(), so the group *is* the control.
--
-- THE MODEL
-- ---------
--   finguard_fraud_analysts    read silver + gold + marts, PAN masked.
--                              The default seat. Everything needed to
--                              investigate an alert, nothing more.
--
--   finguard_pci_privileged    unmasked PAN. Deliberately no extra table
--                              access -- this group grants *visibility into a
--                              field*, not broader reach. A member still
--                              needs analyst rights to query the table at
--                              all. Two separate questions: may you read
--                              this table, and may you see the PAN in it.
--
--   finguard_engineers         read everything including bronze and ops.
--                              Bronze holds raw Kafka envelopes and is where
--                              replay and debugging happen.
--
-- WHAT IS DELIBERATELY ABSENT
-- ---------------------------
-- No write grants to any human group. Every table in this platform is written
-- by a pipeline or by dbt, both running as a service identity. A human with
-- INSERT on silver.transactions is a human who can corrupt the audit record
-- of a fraud investigation. Writes belong to the deployment path, not to
-- people -- and if a human needs to fix data, that should be a reviewed
-- change to code, not an ad-hoc UPDATE.
--
-- PREREQUISITE: the three groups must exist. See 01_pii_masking.sql.
-- Statements are idempotent; GRANT on an already-granted privilege is a no-op.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. Catalog-level visibility
--
-- USE CATALOG is necessary but not sufficient -- it permits traversal, not
-- reading. Without it a group cannot reach the schemas below no matter what
-- is granted there.
-- ---------------------------------------------------------------------------

GRANT USE CATALOG ON CATALOG finguard TO `finguard_fraud_analysts`;
GRANT USE CATALOG ON CATALOG finguard TO `finguard_pci_privileged`;
GRANT USE CATALOG ON CATALOG finguard TO `finguard_engineers`;


-- ---------------------------------------------------------------------------
-- 2. Fraud analysts: silver, gold, marts. Read only. PAN masked.
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA finguard.silver TO `finguard_fraud_analysts`;
GRANT USE SCHEMA ON SCHEMA finguard.gold   TO `finguard_fraud_analysts`;
GRANT USE SCHEMA ON SCHEMA finguard.marts  TO `finguard_fraud_analysts`;

GRANT SELECT ON SCHEMA finguard.silver TO `finguard_fraud_analysts`;
GRANT SELECT ON SCHEMA finguard.gold   TO `finguard_fraud_analysts`;
GRANT SELECT ON SCHEMA finguard.marts  TO `finguard_fraud_analysts`;

-- Analysts must be able to execute the masking functions.
--
-- This is the trap in Unity Catalog column masks and it fails closed in a
-- confusing way: if the caller cannot EXECUTE the masking function, the query
-- errors rather than returning masked data. The table grant looks correct,
-- the mask looks correct, and SELECT fails with a permission error naming a
-- function the analyst never referenced.
GRANT USE SCHEMA ON SCHEMA finguard.security TO `finguard_fraud_analysts`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_pan         TO `finguard_fraud_analysts`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_email       TO `finguard_fraud_analysts`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_raw_payload TO `finguard_fraud_analysts`;


-- ---------------------------------------------------------------------------
-- 3. PCI-privileged: the same function access, no additional tables.
--
-- Membership changes what mask_pan() *returns*; it does not widen reach.
-- Keeping the two orthogonal means "who can see PANs" is answerable by
-- listing one small group, rather than by reasoning about the union of
-- several grants.
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA finguard.security TO `finguard_pci_privileged`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_pan         TO `finguard_pci_privileged`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_email       TO `finguard_pci_privileged`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_raw_payload TO `finguard_pci_privileged`;


-- ---------------------------------------------------------------------------
-- 4. Engineers: everything readable, including bronze and ops.
--
-- Bronze is included because that is where replay happens -- an engineer
-- diagnosing a bad transaction needs the raw Kafka envelope with its
-- partition and offset. Note this does NOT exempt them from the masks:
-- bronze.transactions stores the payload in a `value` column that is not
-- masked, which is a genuine remaining exposure and is recorded as such in
-- 03_pii_verification.sql rather than quietly ignored.
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA finguard.bronze TO `finguard_engineers`;
GRANT USE SCHEMA ON SCHEMA finguard.silver TO `finguard_engineers`;
GRANT USE SCHEMA ON SCHEMA finguard.gold   TO `finguard_engineers`;
GRANT USE SCHEMA ON SCHEMA finguard.marts  TO `finguard_engineers`;
GRANT USE SCHEMA ON SCHEMA finguard.ops    TO `finguard_engineers`;

GRANT SELECT ON SCHEMA finguard.bronze TO `finguard_engineers`;
GRANT SELECT ON SCHEMA finguard.silver TO `finguard_engineers`;
GRANT SELECT ON SCHEMA finguard.gold   TO `finguard_engineers`;
GRANT SELECT ON SCHEMA finguard.marts  TO `finguard_engineers`;
GRANT SELECT ON SCHEMA finguard.ops    TO `finguard_engineers`;

GRANT USE SCHEMA ON SCHEMA finguard.security TO `finguard_engineers`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_pan         TO `finguard_engineers`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_email       TO `finguard_engineers`;
GRANT EXECUTE ON FUNCTION finguard.security.mask_raw_payload TO `finguard_engineers`;


-- ---------------------------------------------------------------------------
-- 5. Revoke the blanket BROWSE.
--
-- `account users BROWSE` predates this model and grants every user in the
-- account the ability to enumerate finguard's objects. Object names are
-- themselves information -- a schema called `pci` or a table called
-- `blocked_cards` tells an attacker where to aim.
--
-- COMMENTED OUT ON PURPOSE. Running it in a single-user workspace can remove
-- your own path to the catalog if your access derives from `account users`
-- rather than from an explicit grant. Verify you hold access through one of
-- the three groups above, then uncomment.
-- ---------------------------------------------------------------------------

-- REVOKE BROWSE ON CATALOG finguard FROM `account users`;
