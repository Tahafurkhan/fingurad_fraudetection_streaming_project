# Data Governance — PII, Classification and Access

**Scope:** Unity Catalog column masking, data classification, the access model,
and how each is verified.
**Status:** applied and verified against the live workspace on 11 August 2026.

---

## The problem, stated plainly

A card-fraud detection platform stored full 16-digit primary account numbers in
clear text, readable by any principal holding `SELECT`.

```sql
SELECT card_number FROM finguard.silver.transactions LIMIT 1;
-- 4144 88** **** 9904   <- all 16 digits returned; redacted in this doc
```

```sql
SELECT count(*) FROM system.information_schema.column_masks WHERE table_catalog='finguard';
-- 0
SELECT count(*) FROM system.information_schema.column_tags  WHERE catalog_name='finguard';
-- 0
SHOW GRANTS ON CATALOG finguard;
-- account users | BROWSE | CATALOG | finguard
```

No masks, no classification, and one blanket grant. For a system whose purpose is
protecting cardholders, this is not an incomplete feature — it is a contradiction.

### The false sense of security

The project believed it had PAN protection. `dim_customer` contains:

```sql
-- PAN is masked here rather than in staging: staging keeps the full value so
-- the fraud-watchlist join still works on entity_id, and the dimension exposes
-- only what an analyst needs. A column mask in Unity Catalog is the stronger
-- control; this is defence in depth, not a substitute.
concat('****-****-****-', right(card_number, 4)) as card_number_masked
```

The reasoning is sound and the comment correctly identifies the UC mask as the
stronger control. **That control was never applied.** So this was not defence in
depth — it was a single layer, on the one table an analyst is least likely to
open while working a live alert.

This is worth stating because it is the common shape of security debt: not an
absent control, but a *partial* control whose documentation describes the
complete design.

---

## Why the mask belongs in Unity Catalog, not in the model

| | Model-level masking | Unity Catalog column mask |
|---|---|---|
| Protects | one model's output | the column itself |
| Applies to | readers of that model | every engine: SQL, notebook, dbt, JDBC, BI |
| New consumer added | unprotected until someone remembers | protected automatically |
| Bypass | read the source table | requires group membership |
| Visible in | model source | `information_schema.column_masks` |

The decisive row is the third. A model-level mask requires every future
consumer to opt in. A catalog mask makes protection the default and exposure the
exception — which is the only orientation that survives contact with a team.

---

## Design

### Two groups, two orthogonal questions

```
finguard_fraud_analysts     read silver + gold + marts, PAN masked
finguard_pci_privileged     unmasked PAN, no additional table access
finguard_engineers          read everything including bronze + ops
```

The separation is deliberate. `finguard_pci_privileged` grants **visibility
into a field**, not reach. A member still needs analyst rights to query the
table at all. That keeps two questions independent:

- *May you read this table?* → table grants
- *May you see the PAN in it?* → group membership

The payoff is auditability: "who can see PANs" is answered by listing one small
group, rather than by reasoning about the union of several grants.

### Fails closed

`is_account_group_member()` returns FALSE for a group that does not exist rather
than raising. A missing group therefore means **everyone sees the masked
value**. New users are safe before anyone configures them.

A mask that defaults to exposing is a mask that leaks the first time someone is
onboarded in a hurry.

### The functions

```sql
CREATE OR REPLACE FUNCTION finguard.security.mask_pan(pan STRING)
COMMENT 'PCI-DSS 3.3 display masking. Last four digits only...'
RETURN
    CASE
        WHEN is_account_group_member('finguard_pci_privileged') THEN pan
        WHEN pan IS NULL THEN NULL
        WHEN length(pan) < 8 THEN '****'
        ELSE concat('****-****-****-', right(pan, 4))
    END;
```

Three details are load-bearing:

**NULL in, NULL out.** Without this branch a NULL PAN becomes the literal string
`'****-****-****-'`, which reads as a real but empty card and breaks `IS NULL`
checks downstream. Masking must preserve absence.

**The `length < 8` guard.** `right(pan, 4)` on a four-character value returns the
entire value. The guard prevents a short value being revealed in full by the
function meant to protect it.

**Return type must match column type.** A type mismatch is rejected at `ALTER`
time — the good case. The bad case is a mask returning a wider compatible type
that silently changes downstream behaviour.

### Functions live in their own schema

`finguard.security` rather than beside the data, so the permission to **alter a
control** is separable from the permission to **read the data it protects**.
Anyone who can edit the mask can disable it.

---

## What is masked

| Table | Column | Function |
|---|---|---|
| `silver.transactions` | `card_number` | `mask_pan` |
| `silver.customers` | `card_number` | `mask_pan` |
| `silver.customers` | `email` | `mask_email` |
| `silver.transactions_quarantine` | `card_number` | `mask_pan` |
| `silver.transactions_quarantine` | `raw_payload` | `mask_raw_payload` |
| `silver.customers_quarantine` | `card_number` | `mask_pan` |
| `gold.fraud_card_alert` | `card_number` | `mask_pan` |
| `gold.fraud_card_alert` | `customer_email` | `mask_email` |
| `bronze.customers` | `card_number` | `mask_pan` |
| `snapshots.customers_snapshot` | `card_number` | `mask_pan` |

**Ten masks.** The last two were not in the original plan — see below.

### Why `raw_payload` is withheld entirely

The quarantine table keeps the verbatim Kafka message so a rejected row can be
traced to its offset and replayed. That payload contains the PAN inside a JSON
string, and **a column mask cannot reach inside a value**. Masking
`card_number` while the raw JSON sat beside it in clear would be theatre, so the
whole column is withheld from non-privileged callers:

```
[REDACTED - contains unparsed PAN, privileged access required]
```

---

## The finding that justifies the verification file

`03_pii_verification.sql` CHECK 4 searches for PAN-shaped column names
**regardless of whether anyone classified them**. It was written on the
assumption that the first pass had missed something.

Run immediately after the first six masks were applied *and confirmed working*,
it found two more:

```
bronze.customers.card_number              5026 54** **** 1551   CLEAR
snapshots.customers_snapshot.card_number  5212 34** **** 1234   CLEAR
```

**Why `bronze.customers` was missed.** The threat model was "transactions carry
PANs." But the customer master carries the card on file, and CDC lands it in
bronze before silver ever runs.

**Why `snapshots.customers_snapshot` was missed.** dbt writes it, so it sat
outside the mental model of "tables the pipeline owns." It is also the worst one
to miss: an SCD2 snapshot retains **every historical version forever**, so it
accumulates PANs that no longer exist anywhere else.

The marts were checked the same way and needed nothing — `stg_*` are VIEWs and
inherit the source mask, `dim_customer.card_number_masked` was already derived.
Verified by reading actual values, not by reasoning about view semantics.

**The lesson:** a check that validates what you built confirms your assumptions.
A check that hunts for what you missed finds bugs.

---

## Verification

Six checks in `sql/governance/03_pii_verification.sql`. Results against the
live workspace:

| Check | Question | Result |
|---|---|---|
| 1 | Are the masks attached? | **10** — PASS |
| 2 | Does the mask transform the value? | `****-****-****-9904` — PASS |
| 3 | Anything tagged sensitive but unmasked? | **0** — PASS |
| 4 | PAN-shaped columns nobody classified? | 2 found, both fixed |
| 5 | Did masking break the fraud join? | 375 = 375 — PASS |
| 6 | Do masks survive a rebuild? | detector in place |

### CHECK 5 is the one that could have been a disaster

`gold.fraud_card_alert` joins on the masked column:

```python
transactions.card_number == fraud_watchlist.entity_id
```

If Unity Catalog applied masks to **join inputs** rather than to the output
projection, every comparison would become `'****-****-****-9904' = '4144...'`,
the join would return zero rows, **fraud detection would silently stop**, and
the pipeline would report success.

UC applies masks at projection time, so this is fine. But "this is expected to
be fine" is precisely the phrase that preceded three silent failures in this
project's history, so it is asserted rather than trusted:

```
alert_count = 375   baseline_before_masking = 375   unchanged = true
fct_transactions = 4,396   baseline = 4,396   unchanged = true
```

---

## Residual risk — stated, not hidden

### 1. Masking is not encryption

PCI-DSS 3.3 covers masking on display. **3.5 requires the PAN rendered
unreadable at rest** — tokenization, or strong cryptography with managed keys.

The bytes here remain clear text on storage. The mask is an access-time
transformation applied by the query engine. Anyone reading the Delta files
outside Unity Catalog still sees the PAN.

**This platform is not PCI compliant, and this document does not claim it is.**

### 2. `bronze.transactions.value` retains PANs in clear

4,432 rows hold the raw Kafka payload with `card_number` inside JSON. A column
mask cannot mask a substring, and masking the column wholesale breaks silver,
which parses it.

The real fix is tokenizing at the producer so the PAN never lands. Until then
this appears in CHECK 4's output rather than being filtered out of the query —
a documented exception is a decision; an exception excluded from the check is a
lie.

### 3. Masks do not survive a full refresh

Lakeflow **drops and recreates** tables on full refresh. A recreated table has
no column masks — no error, no log line, no alert. The `ALTER` statements
applied to a table that no longer exists.

This is not hypothetical: a `bronze.transactions` full refresh is already on the
backlog to fix a stale checkpoint, and running it would un-mask everything
downstream.

**Mitigation:** re-run `01_pii_masking.sql` after any full refresh and confirm
with CHECK 6. **Durable fix:** apply masks from the pipeline itself so they are
part of table creation.

---

## Operational notes

### Prerequisite: the groups must exist

```bash
databricks account groups create --json '{"displayName":"finguard_pci_privileged"}'
databricks account groups create --json '{"displayName":"finguard_fraud_analysts"}'
databricks account groups create --json '{"displayName":"finguard_engineers"}'
```

Until they exist, `is_account_group_member()` returns false for everyone, so
**every caller sees masked values including the table owner**. That is the
fail-closed behaviour working as designed, but it means the privileged path is
untested until a group exists and has a member.

`02_grants.sql` cannot run before the groups exist — it grants to them by name.

### The grant that is easy to miss

```sql
GRANT EXECUTE ON FUNCTION finguard.security.mask_pan TO `finguard_fraud_analysts`;
```

Without `EXECUTE` on the masking function, `SELECT` **errors** rather than
returning masked data — and the error names a function the analyst never
referenced in their query. Table grant correct, mask correct, query fails.

### No write grants to humans

Every table is written by a pipeline or by dbt, both running as a service
identity. A human with `INSERT` on `silver.transactions` can corrupt the audit
record of a fraud investigation. Writes belong to the deployment path. If data
must be fixed, that is a reviewed change to code, not an ad-hoc `UPDATE`.

---

## Interview angles

**"How do you handle PII?"** — Distinguish a control from a transformation.
Masking in a model protects that model's output; masking in the catalog protects
the column through every engine that reads it. Then name the failure mode: a
model-level mask requires every future consumer to opt in.

**"How do you know it works?"** — Six assertions, including one confirming the
fraud join still returns 375 alerts. Masks applied to join inputs would silently
stop fraud detection.

**"Is this compliant?"** — No. 3.3 display masking, yes. 3.5 at-rest, no —
that needs tokenization. Claiming compliance is the fastest way to fail the
round; scoping it precisely is the answer that passes.

**The strongest thing to say** — not "I implemented masking," but: "I
implemented it, then wrote a check that assumed I'd missed something, and it
found two tables I had — a CDC landing table and a dbt snapshot. The snapshot
was the worst, because it retains every historical version forever." Then
explain *why* they were missed. That is a description of how security gaps
actually happen, and it is far more convincing than a clean success.

---

## Files

| File | Purpose |
|---|---|
| `sql/governance/01_pii_masking.sql` | Functions, 10 masks, 28 column tags + 10 table tags |
| `sql/governance/02_grants.sql` | Three-group access model |
| `sql/governance/03_pii_verification.sql` | 6 assertions the controls work |

All committed rather than clicked into the workspace — for the same reason the
Asset Bundle replaced the sync script and `create_alerts.py` replaced UI-configured
alerts: **workspace state that matters must be reviewable in a diff.**
