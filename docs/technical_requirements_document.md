# Technical Requirements Document — FinGuard Real-Time Fraud Detection Platform

| Field | Value |
|---|---|
| **Document ID** | FG-TRD-001 |
| **Version** | 1.2 |
| **Status** | Approved — Baselined for Build |
| **Classification** | Internal — Restricted (contains PCI-DSS scope definitions) |
| **Author** | Taha Furkhan — Lead Data Engineer |
| **Programme** | Card Fraud Detection Modernisation |
| **Supersedes** | FG-BRD-001 §7 (technical appendix), withdrawn |

### Revision history

| Ver | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-05-02 | Lead DE | Initial draft from BRD-001 |
| 0.5 | 2026-05-14 | Lead DE | Added NFRs after latency workshop with Fraud Ops |
| 0.9 | 2026-05-28 | Lead DE | PCI scope revised following Compliance review — §8 rewritten |
| 1.0 | 2026-06-04 | Lead DE | Baselined. Sign-off §16 |
| 1.1 | 2026-07-19 | Lead DE | **Change request CR-004**: watermark/latency conflict formally accepted — see §5.3 |
| 1.2 | 2026-08-11 | Lead DE | **CR-007**: `fail_on_data_loss` retained post-incident; §11.4 added |
| 1.3 | 2026-08-11 | Lead DE | **CR-009**: FR-11 found unenforced and closed; three producer guarantees found unchecked — see §9.4 |

> **A note on versions 1.1 and 1.2.** Both are amendments raised *after* build
> started, because the build disproved something in the baseline. They are
> recorded here rather than quietly corrected. A design document that never
> changes was either never used or is no longer true.

---

## Table of contents

| § | Section |
|---|---|
| 1 | Purpose and scope |
| 2 | Stakeholders and RACI |
| 3 | Business context and drivers |
| 4 | Functional requirements |
| 5 | Non-functional requirements |
| 6 | Data contracts and source specifications |
| 7 | Target architecture |
| 8 | Security, PCI-DSS and data protection |
| 9 | Data quality requirements |
| 10 | Observability and operational requirements |
| 11 | Resilience, DR and failure handling |
| 12 | Cost model and constraints |
| 13 | Test strategy |
| 14 | Assumptions, dependencies and risks |
| 15 | Out of scope and future phases |
| 16 | Approval and sign-off |
| 17 | Appendix A — Requirements traceability matrix |
| 18 | Appendix B — Build authorship statement |

---

## 1. Purpose and scope

### 1.1 Purpose

This document translates the business requirements in **FG-BRD-001** into an
implementable technical specification. It is the controlling artefact for the
build: where this document and the implementation disagree, one of them is a
defect, and the disagreement is resolved by change request rather than by
whichever was written most recently.

It is written for three audiences:

- **Engineering** — as the specification to build against
- **Compliance and InfoSec** — as the statement of what data goes where, and what protects it
- **Operations** — as the source of the SLAs they will be held to at 02:00

### 1.2 In scope

| Ref | Scope item |
|---|---|
| SC-01 | Ingestion of card transaction events from the Payments Platform Kafka topic |
| SC-02 | Ingestion of customer master data from the core banking Postgres instance via CDC |
| SC-03 | Ingestion of the Fraud Operations watchlist and merchant reference files |
| SC-04 | Real-time detection of two alert classes: watchlisted-card usage, and per-customer limit breach |
| SC-05 | A curated dimensional layer supporting fraud analytics and regulatory reporting |
| SC-06 | Masking of cardholder data sufficient to keep the analytical platform out of PCI-DSS CDE scope |
| SC-07 | Operational telemetry sufficient to diagnose a failure without access to the vendor console |

### 1.3 Out of scope

See §15 for the full list with rationale. Summarised: no ML scoring model, no
case-management workflow, no transaction blocking (this platform *detects*; it
does not *decline*), no cardholder-facing notification.

> **The scope boundary that matters most:** this platform is **detective, not
> preventive**. It observes transactions that have already been authorised. Any
> requirement implying an inline authorisation decision belongs to the Payments
> Platform team and is explicitly rejected here — see RISK-03.

---

## 2. Stakeholders and RACI

### 2.1 Stakeholder register

| Role | Responsibility on this programme | Engagement |
|---|---|---|
| **Head of Fraud Operations** | Business sponsor. Owns the loss figure that justifies the spend and accepts the residual risk. | Steering, monthly |
| **Fraud Operations Analysts** | End users. Receive and action alerts. Own the tolerable false-positive rate. | Requirements workshops; UAT |
| **Business Analyst** | Author of FG-BRD-001. Arbitrates conflicting business requirements. | Daily during elaboration |
| **Lead Data Engineer** *(author)* | Owns this document, the pipeline implementation and the data model. | Full-time |
| **Data Architect** | Approves the target architecture against enterprise standards. Owns the medallion pattern mandate. | Design review gates |
| **Payments Platform Team** | Upstream producer. Owns the Kafka topic, its schema and its retention. | Data contract negotiation; change notification |
| **Core Banking DBA** | Owns the Postgres source. Grants CDC access; guarantees cursor-column integrity. | Onboarding; change notification |
| **Compliance / PCI-DSS QSA liaison** | Determines whether the masking design removes this platform from CDE scope. **Holds veto.** | Design review; pre-production audit |
| **Data Protection Officer** | GDPR lawful basis, retention limits, erasure mechanics. **Holds veto.** | Design review |
| **InfoSec** | Secret management, network controls, Unity Catalog privilege model. | Design review; penetration test |
| **Data Steward** | Owns business definitions — what constitutes a "customer", a "merchant", an "alert". | Elaboration |
| **Platform Engineering** | Databricks workspace, Unity Catalog metastore, CI runners, secret scopes. | Provisioning; ongoing |
| **SRE / Data Operations** | Inherits the platform on go-live. Runs the 02:00 runbook. | Handover; ongoing |
| **BI & Reporting** | Downstream consumer of the marts layer. | Post-MVP |

### 2.2 RACI matrix

**R** = Responsible · **A** = Accountable · **C** = Consulted · **I** = Informed

| Activity | Sponsor | BA | Lead DE | Architect | Payments | Compliance | DPO | InfoSec | SRE | Fraud Analysts |
|---|---|---|---|---|---|---|---|---|---|---|
| Business case & funding | **A/R** | C | I | I | — | C | C | I | I | C |
| Requirements elicitation | A | **R** | C | I | C | C | C | I | I | **C** |
| **Data contract with Kafka source** | I | C | **R** | C | **A** | I | I | C | I | — |
| CDC access & cursor guarantee | I | I | **R** | C | — | I | C | C | I | — |
| Target architecture | I | I | **R** | **A** | C | C | I | C | C | I |
| **PCI-DSS scope determination** | A | I | C | C | I | **R** | C | C | I | I |
| **GDPR lawful basis & retention** | A | C | C | I | I | C | **R** | I | I | I |
| Masking implementation | I | I | **R** | C | — | **A** | C | C | I | I |
| Alert threshold definition | A | **R** | C | I | — | I | I | I | I | **A** |
| Latency SLA | **A** | R | **R** | C | C | I | I | I | C | **C** |
| Data quality rules | I | C | **R** | C | C | I | I | I | I | C |
| Pipeline implementation | I | I | **R/A** | C | I | I | I | I | C | I |
| Dimensional model | I | C | **R/A** | C | — | I | I | I | I | C |
| Observability & runbook | I | I | **R** | C | — | I | I | I | **A** | I |
| Cost control | **A** | I | **R** | C | — | I | I | I | C | I |
| UAT sign-off | A | R | C | I | — | C | C | I | C | **A** |
| Production handover | I | I | **R** | I | — | I | I | C | **A** | I |

> **Reading the veto rows.** Compliance is **R** for PCI scope determination and
> the Lead DE only **C** — the engineer builds the masking, but does not get to
> declare it sufficient. The same applies to the DPO on retention. On a fraud
> platform these two rows are where projects actually stall, and pretending
> otherwise in a design document is how a build reaches UAT and is then refused
> a production release.

### 2.3 Escalation path

Technical disagreement → Data Architect → Programme Board.
Compliance objection → **no engineering override exists.** Compliance objections
are resolved by changing the design, or by the sponsor formally accepting the
risk in writing.

---

## 3. Business context and drivers

### 3.1 The problem

Extracted from FG-BRD-001 §2, restated in measurable terms:

Fraud detection currently runs as an overnight batch job. A card compromised at
09:00 continues transacting until the batch completes at approximately 03:00 the
following morning — an exposure window of up to **18 hours**. Fraud Operations
report that the majority of loss on a compromised card accrues in the first
hours, so the detection lag, not the detection *logic*, is the dominant loss
driver.

### 3.2 Business drivers

| Ref | Driver | Measure |
|---|---|---|
| BD-01 | Reduce the fraud exposure window | Time from transaction to analyst alert |
| BD-02 | Reduce analyst time spent on manual data assembly | Alerts arrive with customer and merchant context attached |
| BD-03 | Satisfy the regulator's traceability expectation | Every alert reproducible from retained raw data |
| BD-04 | Avoid expanding PCI-DSS cardholder data environment scope | Assessor confirms analytical platform out of CDE |

### 3.3 The requirement that was rejected

Fraud Operations initially requested **transaction blocking** — the platform
declines the transaction rather than alerting on it.

**Rejected**, for a reason worth recording: blocking is an inline authorisation
decision with a hard budget in the tens of milliseconds, sitting on the payment
authorisation path where an outage stops customers buying things. This platform
is an asynchronous analytical system. Placing it on the authorisation path would
make the bank's card processing availability dependent on a Databricks pipeline.

**Agreed alternative:** this platform emits alerts; a future phase may feed a
scoring service that the Payments Platform calls inline. That service is theirs
to own, not ours. See §15, FUT-02.

---

## 4. Functional requirements

Priority: **M** = MoSCoW Must · **S** = Should · **C** = Could

### 4.1 Ingestion

| Ref | Requirement | Pri |
|---|---|---|
| FR-01 | The platform shall ingest card transaction events from the Payments Platform Kafka topic, continuously and without manual intervention. | M |
| FR-02 | Raw ingested payloads shall be persisted **unparsed and unmodified**, together with the full source envelope (topic, partition, offset, timestamp). | M |
| FR-03 | The platform shall ingest customer master data by change data capture, such that every change to a customer attribute is captured, not merely the latest state. | M |
| FR-04 | The platform shall ingest the Fraud Operations watchlist and merchant reference files on arrival, without a scheduled poll. | M |
| FR-05 | Onboarding a new source of an already-supported type shall require configuration only, not new pipeline code. | S |
| FR-06 | Ingestion shall resume from its last committed position after any interruption, without duplicating or skipping records. | M |

> **FR-02 is load-bearing and is frequently challenged.** Storing the raw
> payload appears wasteful. It is what makes BD-03 achievable: when a downstream
> parsing rule is found to be wrong, the correct output can be *recomputed* from
> retained raw data. Without it, a parsing defect is permanent data loss. The
> cost of this requirement is real — see §7.4, where it constrains the physical
> layout of the bronze layer.

### 4.2 Cleansing and conformance

| Ref | Requirement | Pri |
|---|---|---|
| FR-07 | Raw payloads shall be parsed to a typed, explicitly-declared schema. Schema inference is prohibited. | M |
| FR-08 | Records failing a mandatory quality rule shall be **routed to a quarantine table**, not silently discarded. | M |
| FR-09 | Quarantined records shall retain the original payload and the reason for rejection. | M |
| FR-10 | For any ingestion run, `records_in = records_accepted + records_quarantined` shall hold and shall be verifiable by query. | M |
| FR-11 | Duplicate source records shall not produce duplicate rows in the curated layer. | M |

> **FR-08 exists because of a specific failure mode.** The common alternative —
> dropping bad records — makes data quality invisible. A source that begins
> emitting malformed records produces a pipeline that is green, a row count that
> is quietly lower, and nobody asking why. FR-10 is the arithmetic that makes
> the loss impossible to miss.

### 4.3 Detection

| Ref | Requirement | Pri |
|---|---|---|
| FR-12 | The platform shall raise an alert when a transaction uses a card present on the fraud watchlist. | M |
| FR-13 | The platform shall raise an alert when a transaction exceeds the transaction limit **specific to that customer**. | M |
| FR-14 | Alerts shall carry sufficient context (customer, merchant, amount, channel, location) for an analyst to triage without querying another system. | M |
| FR-15 | The platform shall maintain rolling transaction-volume aggregates for trend monitoring. | S |
| FR-16 | Alerts shall be delivered to the Fraud Operations mailbox. | M |

> **FR-13 says "specific to that customer", and that word was contested.** A
> global threshold is far simpler. Fraud Operations rejected it: a £5,000
> transaction is unremarkable for a private-banking customer and highly
> suspicious for a student account. A global threshold produces either alert
> fatigue at the top of the customer base or blindness at the bottom. This
> requires the customer dimension to be joined *before* the threshold is
> evaluated, which is why detection depends on the CDC feed (FR-03) and not on
> the transaction stream alone.

### 4.4 Curated analytics layer

| Ref | Requirement | Pri |
|---|---|---|
| FR-17 | The platform shall present a dimensional model over conformed customer, merchant and date dimensions. | M |
| FR-18 | Customer and merchant dimensions shall preserve attribute history (SCD Type 2). | M |
| FR-19 | Transaction facts shall join to the dimension version that was **effective at transaction time**, not the current version. | M |
| FR-20 | Curated model rebuilds shall be idempotent — rerunning shall not duplicate or corrupt rows. | M |

> **FR-19 is the requirement most often missed and most costly to retrofit.** If
> a customer moved from Chennai to Dubai in June, a transaction in May must
> report against Chennai. Joining on current state silently rewrites history,
> and every trend report built on it is wrong in a way that looks plausible. It
> forces SCD2 (FR-18) and a point-in-time join, and it must be designed in from
> the start.

---

## 5. Non-functional requirements

### 5.1 Latency and throughput

| Ref | Requirement | Target | Measured by |
|---|---|---|---|
| NFR-01 | Alert latency — transaction event time to alert availability | **≤ 15 min (P95)** | Ops telemetry |
| NFR-02 | Curated analytics layer freshness | ≤ 24 h | dbt source freshness |
| NFR-03 | Sustained ingestion throughput at design load | ≥ 500 events/sec | Load test |
| NFR-04 | Ingestion backlog under normal operation | ≈ 0 | `stream_health.num_bytes_outstanding` |

**How NFR-01 was set.** Fraud Operations' opening request was "real time,"
which is not a requirement. The workshop question that produced a number was:
*"An alert arrives. What happens next?"* The answer — it enters a queue an
analyst works through, with a median pickup of several minutes — establishes
that latency below the analyst pickup time delivers no business value. 15
minutes was agreed as comfortably inside the human response loop while allowing
a scheduled rather than always-on execution model, which is materially cheaper
(§12).

### 5.2 Latency budget decomposition

End-to-end latency is the sum of its stages, and one stage dominates:

```
  event produced at source
    ├── source → platform commit ......... seconds
    ├── TRIGGER INTERVAL WAIT ............ 0 .. T          ◄── dominant
    ├── pipeline execution ............... ~4 min observed
    └── watermark delay (join only) ...... up to W
                                          ─────────────────
   P95 ≈ T + execution + W
```

With execution ≈ 4 min and the join watermark W = 5 min, NFR-01 requires
**T ≤ 6 min**; a 5-minute trigger is specified, leaving headroom.

> **The consequence, stated so it is not later mistaken for a defect:** reducing
> T below the watermark W buys nothing for the watchlist alert. Latency for that
> alert is floored by W, and W is a correctness parameter, not a performance
> one. See §5.3.

### 5.3 CR-004 — The latency/completeness conflict (accepted)

Two baselined requirements are in direct tension:

- **FR-12** — alert on watchlisted-card usage
- **NFR-01** — alert within 15 minutes

The watchlist is a *stream*, not a static table. Fraud Operations add a card to
the watchlist *after* discovering compromise — routinely after the fraudulent
transactions have already occurred. Matching those transactions requires the
join to hold state long enough for the late watchlist entry to arrive.

That retention window is the watermark, W. It cannot be both large (catch late
watchlist entries) and small (low latency).

| W | Late watchlist entries caught | Alert latency floor | State memory |
|---|---|---|---|
| 1 min | Almost none | 1 min | Minimal |
| **5 min** | **Modest** | **5 min** | **Bounded** |
| 24 h | Nearly all | 24 h | Unbounded growth |

**Decision (Fraud Ops + Lead DE, 2026-07-19):** W = 5 minutes.
**Formally accepted consequence:** a watchlist entry added more than 5 minutes
after a transaction will **not** retrospectively alert on it. Retrospective
matching is a batch reconciliation concern and is assigned to FUT-03.

> This is recorded as an accepted limitation rather than a bug. It was
> discovered during build, not during design — the design assumed the watchlist
> was reference data. It is not; it is an event stream, and that reclassification
> is what forced CR-004.

### 5.4 Availability and recovery

| Ref | Requirement | Target |
|---|---|---|
| NFR-05 | Platform availability (business hours) | 99.5% |
| NFR-06 | **RPO** — tolerable data loss | **Zero.** Recovery from retained source offsets. |
| NFR-07 | **RTO** — restoration of alerting | ≤ 4 h |
| NFR-08 | Curated layer full-rebuild time | ≤ 8 h |

> **RPO = zero is achievable only because of FR-02 and FR-06.** The source
> retains its own history; the platform records its committed position. Recovery
> is replay, not restore-from-backup. This is the single strongest argument for
> the raw-payload requirement, and it is why NFR-06 and FR-02 must be read
> together.

### 5.5 Scalability, maintainability, portability

| Ref | Requirement |
|---|---|
| NFR-09 | Adding a source of an existing type shall not require pipeline code changes (implements FR-05). |
| NFR-10 | Physical layout shall be changeable without rewriting historical data. |
| NFR-11 | All infrastructure shall be declared as version-controlled code. No console-configured production resources. |
| NFR-12 | Business logic shall be expressed in SQL or PySpark DataFrame APIs, avoiding vendor-proprietary constructs where a portable equivalent exists. |
| NFR-13 | Every environment shall be reproducible from the repository plus a secret store. |

> **NFR-10 has a specific technical consequence** and is not generic good
> practice: it rules out physical partitioning as the clustering strategy,
> because partition schemes cannot be changed retrospectively without a full
> rewrite. It mandates a technique whose keys can be redefined later. This
> requirement was written *because* the access pattern for a fraud platform is
> not knowable in advance.

---

## 6. Data contracts and source specifications

> **A data contract is an agreement between two teams, not a schema file.** Each
> subsection below has a named counterparty who has agreed to it. Where no such
> agreement exists, that is recorded as a risk rather than assumed away.

### 6.1 Transaction event stream

| Attribute | Specification |
|---|---|
| **Counterparty** | Payments Platform Team |
| **Transport** | Managed Kafka, TLS with SASL authentication |
| **Format** | JSON, UTF-8, one event per message |
| **Ordering** | Guaranteed within a partition only. **No global ordering.** |
| **Delivery** | At-least-once. Consumer must tolerate duplicates (FR-11). |
| **Retention** | 7 days |
| **Design volume** | 500 events/sec sustained, 2,000 peak |

**Payload contract — mandatory fields**

| Field | Type | Constraint |
|---|---|---|
| `transaction_id` | string | Unique; **non-null** |
| `customer_id` | string | **Non-null**; FK to customer master |
| `card_number` | string | **Non-null**; PCI-regulated — see §8 |
| `merchant_id` | string | **Non-null**; FK to merchant reference |
| `amount` | decimal | **> 0** |
| `currency` | string | ISO 4217 |
| `transaction_timestamp` | timestamp | ISO 8601 with offset; the event-time field |
| `status` | string | Enumerated |

Optional: `merchant_name`, `merchant_category`, `transaction_type`,
`payment_channel`, `device_id`, `city`, `country`, `is_international`.

**Schema evolution clause** — the negotiated term, and the one that matters:

| Change | Notice | Consumer impact |
|---|---|---|
| Add optional field | 0 days — permitted at will | None; ignored until adopted |
| Add mandatory field | 30 days written | Contract amendment |
| Remove / rename / retype any field | **90 days written, breaking change** | Contract amendment; joint release |

> **Why the asymmetry.** Additive change is safe because the consumer projects
> explicitly (FR-07) and ignores unknown fields. Removal is not, and no amount
> of consumer defensiveness fixes it — a field that stops arriving becomes null,
> nulls fail expectations, and rows route to quarantine (FR-08). The 90-day term
> exists so this is a planned joint release rather than a 02:00 page.

**Open risk:** contract enforcement is currently by agreement and consumer-side
validation, not by a registry that rejects non-conforming producers. See RISK-01.

### 6.2 Customer master (CDC)

| Attribute | Specification |
|---|---|
| **Counterparty** | Core Banking DBA |
| **Mechanism** | Query-based CDC over a monotonic cursor column |
| **Primary key** | `customer_id` |
| **Cursor** | `update_timestamp` |
| **Mutability** | Attributes are mutable at source — drives FR-18 (SCD2) |
| **Credentials** | Held write-only in the platform's connection object; never visible to pipeline code |

> **The guarantee this contract depends on** — and the question a DBA must be
> asked explicitly: *is `update_timestamp` written on **every** update path,
> including bulk corrections and manual data fixes?* If any path bypasses it,
> those changes are invisible to CDC and the SCD2 history is silently
> incomplete. This is assumption ASM-03 and it is untested — see RISK-04.

### 6.3 Fraud watchlist

| Attribute | Specification |
|---|---|
| **Counterparty** | Fraud Operations |
| **Delivery** | File drop to a governed storage location |
| **Trigger** | On arrival — no polling schedule |
| **Semantics** | **Event stream, not reference data** — entries carry an effective-from time |

> **This row is the reclassification that caused CR-004.** The baseline design
> treated the watchlist as a slowly-changing reference table. It is not. Cards
> are added reactively, after compromise is discovered, and the effective-from
> time is materially later than the transactions of interest. Everything in
> §5.3 follows from this single line.

### 6.4 Merchant reference

| Attribute | Specification |
|---|---|
| **Counterparty** | Merchant Services |
| **Delivery** | Periodic full-file drop |
| **Semantics** | Full snapshot — **contains duplicate merchant records by design** |
| **Consumer obligation** | Deduplicate to one row per merchant (FR-11) |

> Verified in build: the received file contained 400 rows across 200 distinct
> merchants. The 50% reduction on load is **conformance, not data loss**, and is
> called out here because it will otherwise be raised as a defect during UAT.

---

## 7. Target architecture

### 7.1 Architectural mandate

The Data Architect mandates a **medallion (multi-hop) architecture** as the
enterprise standard: raw → cleansed → curated, each layer persisted, each
recomputable from the layer above.

### 7.2 Layer specification

| Layer | Contains | Contract | Consumers |
|---|---|---|---|
| **Bronze** | Raw payloads, unparsed, plus source envelope | Append-only. Never edited in place. | Silver; incident replay |
| **Silver** | Typed, validated, deduplicated, conformed | Schema-stable. Quality-enforced. | Gold; data science |
| **Gold** | Detection outputs and aggregates | Business-meaningful. Alert-bearing. | Analysts; marts |
| **Marts** | Dimensional model, SCD2 history | Point-in-time correct | BI; regulatory reporting |
| **Ops** | Run history, quality results, stream telemetry | Append-only time series | SRE; alerting |
| **Security** | Masking functions and grants | Governance-controlled | All layers |

### 7.3 Layer transition rules

| Rule | Statement |
|---|---|
| AR-01 | No layer may be skipped. A consumer requiring bronze data reads it via silver, or documents an exception. |
| AR-02 | Transformation logic lives in exactly one layer. Duplicated logic is a defect. |
| AR-03 | Quality enforcement occurs at the **bronze → silver** boundary. |
| AR-04 | PII masking is enforced **at the storage layer** via governed functions, never in query logic. |
| AR-05 | Any curated object must be reproducible from bronze by rerunning the pipeline. |

> **AR-04 is a security requirement disguised as an architecture rule.** Masking
> applied in a view or a model protects only the consumers who use that object.
> Enforcement bound to the column protects every access path — including the ad
> hoc query by someone who found the underlying table. See §8.3.

### 7.4 A design constraint accepted, not solved

Bronze retains payloads unparsed (FR-02). The direct consequence: **no business
column exists in bronze to organise the data by.** The natural physical
organisation keys — customer, transaction time — are inside an unparsed string.

Bronze physical layout is therefore keyed on the source envelope (ingest
timestamp, source partition) — the fields actually available, matching bronze's
real access pattern of time-bounded replay and offset-range investigation.
Business-key organisation begins at silver, where those columns exist.

> Recorded because it looks like an oversight in review and is not: it is FR-02
> being paid for. The alternative — parsing in bronze to enable better
> clustering — would forfeit the replay guarantee that NFR-06 (RPO = zero)
> depends on. The constraint was accepted knowingly.

---

## 8. Security, PCI-DSS and data protection

> **Section owner: Compliance.** The Lead DE is Consulted, not Responsible
> (§2.2). Content here is the *design submitted for* assessment, not a
> compliance determination.

### 8.1 Data classification

| Class | Examples | Handling |
|---|---|---|
| **Restricted — PCI** | Primary Account Number (`card_number`) | Masked at storage; unmasked access restricted and logged |
| **Restricted — PII** | Name, email, address, date of birth | Masked by default; unmasked by role |
| **Internal** | Merchant reference, transaction amount, channel | Standard platform controls |
| **Operational** | Run metrics, quality results | Broadly readable |

### 8.2 PCI-DSS scope objective

**Objective (BD-04):** the analytical platform shall remain **outside** the
Cardholder Data Environment.

**Design intent:** the PAN is truncated at rest such that stored values are not
cardholder data as defined by the standard. Full PAN is never persisted in the
curated layers, never present in alert output, and never emitted to the
notification channel.

> **The most important sentence in this document:** *this is a design objective
> submitted for assessment, not a compliance claim.* Only a Qualified Security
> Assessor determines CDE scope. An engineering document that asserts "we are
> PCI compliant" is making a claim it has no authority to make, and stating that
> plainly is itself a compliance control.

### 8.3 Masking requirements

| Ref | Requirement |
|---|---|
| SR-01 | PAN shall be truncated to a non-reconstructable form for all standard access. |
| SR-02 | Masking shall be enforced by **governed functions bound to the column**, not by view definitions or model logic (implements AR-04). |
| SR-03 | Unmasked access shall be role-restricted, granted by exception, time-bounded and logged. |
| SR-04 | Masking shall be **independently verifiable** by an automated test asserting no unmasked value is retrievable by a standard-privilege principal. |
| SR-05 | Alert notifications shall contain no unmasked PAN or PII. |
| SR-06 | Quarantine tables retain original payloads and shall carry the **same** controls as the tables they protect. |

> **SR-04 exists because masking that is not tested is not a control, it is an
> intention.** A single new table, view or export can bypass a masking scheme
> silently — the failure is invisible precisely because the data still looks
> present. The verification must assert absence from the *consumer's*
> perspective, not the presence of a function definition.
>
> **SR-06 is the clause most often missed entirely.** Quarantine holds rejected
> records *including their original raw payload* — the same PANs, in a table
> created for operational convenience and frequently forgotten by the access
> model. It is the natural back door around the entire masking design.

### 8.4 Access control

| Ref | Requirement |
|---|---|
| SR-07 | Access shall be granted to groups, never to individuals. |
| SR-08 | Privilege shall be least-privilege by layer: analysts read gold and marts; engineers read silver; bronze is restricted. |
| SR-09 | Credentials shall be held in a managed secret store, never in source control, configuration files or notebooks. |
| SR-10 | Access grants shall be declared as code and reviewed as code. |

### 8.5 Data protection (GDPR)

> **Section owner: DPO.**

| Ref | Requirement |
|---|---|
| DP-01 | Lawful basis for processing shall be documented — anticipated as legitimate interest in fraud prevention, subject to DPO confirmation. |
| DP-02 | Personal data retention shall be limited to the documented period; expiry shall be enforced, not aspirational. |
| DP-03 | The platform shall support erasure requests, including in append-only layers. |
| DP-04 | Cross-border transfer implications shall be assessed. |

> **DP-03 is in direct tension with FR-02 and AR-05.** Erasure requires deleting
> from an append-only layer whose whole purpose is immutability; RPO = zero
> requires that layer to be replayable. Reconciling these — likely by
> crypto-erasure or a documented exception for fraud-prevention data — is
> assigned to the DPO and is **open at baseline** (RISK-05). It is recorded here
> because discovering it after go-live is considerably worse.

---

## 9. Data quality requirements

### 9.1 Dimensions and rules

| Ref | Dimension | Requirement | On failure |
|---|---|---|---|
| DQ-01 | Completeness | Mandatory contract fields (§6.1) shall be present | Quarantine |
| DQ-02 | Validity | `amount > 0` | Flag and monitor |
| DQ-03 | Uniqueness | One row per `transaction_id` in curated layers | Deduplicate |
| DQ-04 | Referential integrity | Every `customer_id` / `merchant_id` resolves to a dimension | Flag; route to unknown member |
| DQ-05 | Timeliness | Source freshness within contracted bounds | Warn — non-blocking |
| DQ-06 | Consistency | `in = accepted + quarantined` (FR-10) | **Fail the run** |
| DQ-07 | Schema conformance | No unannounced structural drift | Quarantine and alert |

### 9.2 Enforcement severity — the design decision

Three severities, deliberately distinguished:

| Severity | Behaviour | Applied to | Rationale |
|---|---|---|---|
| **Quarantine** | Row removed from the good path, retained with reason | DQ-01, DQ-07 | Record is unusable but must not vanish |
| **Flag** | Row proceeds; violation recorded and counted | DQ-02, DQ-04, DQ-05 | Suspicious ≠ invalid. A zero-amount transaction may be a legitimate reversal. |
| **Fail** | Run halts | DQ-06 | Reconciliation failure means the platform cannot account for its own data |

> **Why `amount > 0` is Flag and not Quarantine.** A quarantining rule silently
> removes rows, and if that rule is wrong the loss is invisible until someone
> reconciles against source. Zero and negative amounts appear legitimately as
> reversals and adjustments. Quarantining them would delete real transactions
> and under-report fraud exposure. The rule is retained for *visibility*
> because a sudden spike in violations indicates an upstream defect — which is
> exactly what a counted flag surfaces and a silent drop conceals.

### 9.3 Reconciliation

| Ref | Requirement |
|---|---|
| DQ-08 | Every ingestion run shall record: records read, accepted, quarantined, and the source position range consumed. |
| DQ-09 | Reconciliation shall be queryable by SRE without vendor console access. |
| DQ-10 | An unexplained discrepancy between source and platform counts shall raise an alert. |

### 9.4 CR-009 — Two requirements found stated but unenforced

Raised after an automated contract-alignment check was built. Both findings
share a shape worth naming: **the requirement held in production and nothing
was making it hold.**

**Finding 1 — FR-11 (no duplicates) was unenforced.**

The Kafka contract is at-least-once (§6.1), so retries and replays deliver the
same transaction twice as normal operation. Silver had no deduplication.
Measured at the time: 825 rows, 825 distinct `transaction_id`. Zero duplicates
— because no retry had yet occurred, not because anything prevented one.

*Resolution:* `dropDuplicatesWithinWatermark` on `transaction_id`, watermarked
on `transaction_timestamp` (event time, so a replay lands in the same window as
the original). Ten minutes, taken from the failure mode: at-least-once
duplicates are retries and arrive within seconds.

**Finding 2 — three producer guarantees had no consumer rule.**

`transaction_timestamp`, `currency` and `status` are `required` in the producer
contract, and no drop or flag rule checked any of them. All three were fully
populated in production.

*Resolution, deliberately not uniform:*

| Field | Severity | Why |
|---|---|---|
| `transaction_timestamp` | **Drop** | The event-time column. A null cannot be placed in any window or advance a watermark, so it degrades the batch, not just the row. |
| `currency`, `status` | **Flag** | A transaction missing its currency is still a real transaction against a real card. Dropping it would delete fraud evidence to satisfy a metadata rule. |

> **The generalisable point, and the reason this is a change request rather
> than a silent fix.** An unenforced guarantee looks identical to an enforced
> one for as long as the producer behaves. Neither of these would have appeared
> in any dashboard, because both were being satisfied. Only a check that
> compares the *stated* contract against the *implemented* rules can find the
> difference — which is why TS-02 is the highest-value test in §13, and why
> RISK-01 stays open.

---

## 10. Observability and operational requirements

### 10.1 Principle

> **The platform must be diagnosable from data it has persisted itself.** An
> engineer at 02:00 must reach a diagnosis by querying tables, not by clicking
> through a vendor UI whose retention they do not control. This is a
> requirement, not a preference: console retention is finite, and the run that
> matters is often older than it.

### 10.2 Requirements

| Ref | Requirement |
|---|---|
| OB-01 | Every pipeline run shall persist its outcome, timing and failure message to a queryable table. |
| OB-02 | Every quality rule evaluation shall persist pass/fail counts per run. |
| OB-03 | Streaming operators shall persist watermark position, state size, late-record counts, batch duration and source backlog. |
| OB-04 | Telemetry collection shall run **whether the pipeline succeeded or failed**. |
| OB-05 | Telemetry collection failure shall never fail the run it observes. |
| OB-06 | Telemetry collection shall be incremental and idempotent. |
| OB-07 | Alerting shall distinguish **page** (act now) from **notify** (review in hours). |
| OB-08 | A runbook shall exist for each paging alert, containing diagnosis steps and remediation. |

> **OB-04 is the one that is almost always got wrong.** Telemetry that runs only
> on success collects evidence for every case except the one requiring
> diagnosis. It must be conditioned on *completion*, not on *success*.
>
> **OB-05 is its necessary counterweight.** A pipeline failing because its own
> monitoring failed is worse than no monitoring — it trains operators to ignore
> red, and then they ignore the real one.
>
> **OB-01 says *every* run, and that word did real work.** The original
> implementation mined the Lakeflow event log, which covers everything inside a
> pipeline update and nothing outside one — leaving the dbt tasks, tasks skipped
> because an upstream one failed, runs killed by timeout, and runs that never
> fired at all with no record anywhere. The blind spot is worse than it sounds:
> if the scheduled job stops firing entirely, the pipeline event log stays quiet
> and every detector reads healthy. **Silence is indistinguishable from success
> when you only watch the thing that did not run.** Closed by a second collector
> reading the Jobs API into `ops.job_runs` and `ops.job_task_runs`.

### 10.4 SLA measurement

| Ref | Requirement |
|---|---|
| OB-09 | Each SLA in §5 shall be measured per run and persisted, not merely stated. |
| OB-10 | Latency shall be decomposed into queue and execution time. |

> **A target nobody measures is an aspiration.** NFR-01 specified 15 minutes and
> for most of this project's life nothing recorded whether a run met it.
> `ops.job_sla` now records per-run latency against the target.
>
> **What that table measures is deliberately narrower than NFR-01, and must not
> be read as the full figure.** Its clock starts when the run is *scheduled*;
> the SLA's clock starts when the *transaction occurred*. Missing from it: the
> time an event waited in Kafka before the run triggered (the dominant term,
> §5.2) and the join watermark delay (up to 5 minutes, §5.3). A run comfortably
> inside 900s therefore does not prove the SLA is met. What it does prove is the
> controllable part — if execution alone approaches the target, no trigger
> interval can rescue it. True end-to-end measurement needs event-time-to-alert
> per row and is assigned to FUT-06.

### 10.5 Lineage

| Ref | Requirement |
|---|---|
| OB-11 | Table and column lineage shall be queryable, not only browsable in a UI. |
| OB-12 | Downstream impact of a change shall be answerable by query, transitively. |
| OB-13 | Architecture layer rules (§7.3) shall be checked against actual lineage, not assumed. |

> **OB-12 exists to make the §6.1 notice period usable.** A 90-day warning about
> a removed field is worth nothing if answering "what does that field feed?"
> means clicking through four layers and hoping no branch was missed.
>
> **OB-13 turns AR-01 from prose into a check.** An architecture rule nothing
> verifies is a preference. Note the retention caveat: lineage records what has
> *run recently*, so a quarterly job outside the window is invisible and a
> schema deleted last month still appears.

### 10.3 Alert routing

| Condition | Severity | Rationale |
|---|---|---|
| Ingestion halted / detection stopped | **Page** | Fraud is undetected right now |
| Reconciliation failure (DQ-06) | **Page** | Platform cannot account for its data |
| Quarantine rate exceeds threshold | Notify | Indicates upstream change; not immediate loss |
| Source freshness breach | Notify | Usually a quiet upstream, not a defect |
| Cost variance | Notify (daily) | No hourly action exists |

> **The last row is deliberate.** Cost alerts fire daily, not hourly, because
> there is no remediation an engineer can perform at 03:00 in response to a cost
> spike. An alert with no action attached is noise, and noise is what makes
> operators ignore the alert that mattered.

---

## 11. Resilience, DR and failure handling

### 11.1 Failure modes and required responses

| Failure | Required platform behaviour | Recovery |
|---|---|---|
| Source unavailable | Fail visibly; do not advance position | Resume from committed position |
| Source outage < retention | **No data loss**; latency breach only | Automatic on restoration |
| Source outage > retention | **Detect and refuse to proceed silently** | Documented recovery; loss quantified |
| Malformed records | Quarantine; continue | Reprocess after fix |
| Unannounced schema change | Quarantine; alert | Contract escalation (§6.1) |
| Credential expiry | Fail visibly with a clear cause | Rotate; resume |
| Downstream store unavailable | Retry with backoff; then fail | Automatic |

### 11.2 Recovery position handling

| Ref | Requirement |
|---|---|
| RS-01 | Ingestion shall commit its source position only after data is durably persisted. |
| RS-02 | The platform shall **fail loudly if its committed position no longer exists at source**. |
| RS-03 | Reprocessing from an arbitrary position shall be possible without code changes. |

### 11.3 Backfill

| Ref | Requirement |
|---|---|
| RS-04 | Each layer shall be rebuildable from the layer beneath it, independently. |
| RS-05 | Backfill shall be idempotent — repeated runs converge to the same result. |
| RS-06 | Backfill shall not require the source to be replayed if the raw layer already holds the data. |

> **RS-06 is FR-02 paying for itself a second time.** Because bronze retains raw
> payloads, a silver-layer logic defect is repaired by rebuilding silver from
> bronze — no upstream involvement, no dependence on source retention.

### 11.4 CR-007 — `fail_on_data_loss` retained after incident

**Background.** RS-02 requires loud failure when a committed position no longer
exists at source. During build, a source-side topic recreation triggered exactly
this failure. The standard vendor workaround is to disable the check.

**Decision: rejected. RS-02 stands unchanged.**

**Rationale.** That setting is the *only* mechanism distinguishing "the source
was recreated and we knowingly accept a gap" from "we are silently skipping
transactions." Disabling it converts a loud, correct failure into permanent
blindness — and on a fraud platform, silently skipped transactions are
undetected fraud. The incident was resolved by an explicit, recorded
reprocessing decision, which is what RS-02 is *for*.

> Recorded as a change request because the pressure to flip that setting recurs
> every time the failure occurs, usually at 02:00, usually from someone who
> wants the pipeline green. The rationale needs to be findable then.

### 11.5 DR targets

| Scenario | RPO | RTO |
|---|---|---|
| Pipeline failure | 0 | ≤ 1 h |
| Workspace loss | 0 (replay from source + raw layer) | ≤ 4 h |
| Region loss | 0 | ≤ 24 h (accepted — see RISK-06) |
| Logic defect requiring rebuild | 0 | ≤ 8 h |

---

## 12. Cost model and constraints

### 12.1 Requirements

| Ref | Requirement |
|---|---|
| CO-01 | Compute shall scale to zero when idle. No always-on cluster shall be provisioned for a workload that is not continuous. |
| CO-02 | All resources shall carry attribution tags (project, environment, cost centre, owner). |
| CO-03 | Runs shall carry a timeout bounding the spend of a hung run. |
| CO-04 | Cost variance beyond an agreed threshold shall raise a daily notification. |

### 12.2 The execution model decision

| Model | Latency | Compute profile | Meets NFR-01? |
|---|---|---|---|
| Batch, daily | ~24 h | Minimal | ✗ |
| **Scheduled micro-batch (5 min)** | **~10 min** | **Runs only when triggered** | **✓** |
| Continuous streaming | ~seconds | **Always on, 24/7** | ✓ (over-delivers) |

**Selected: scheduled micro-batch.**

> **The reasoning, which is a business argument rather than a technical one.**
> Continuous execution delivers sub-minute latency at the cost of compute that
> never scales to zero. NFR-01 requires 15 minutes, and §5.1 established that
> latency below analyst pickup time delivers no business value. Paying
> continuously for latency nobody consumes is waste. Further, per §5.3, the
> watchlist alert is floored at the 5-minute watermark regardless — so
> continuous mode cannot deliver its headline benefit for the primary detection
> use case anyway.
>
> **CO-03 exists because "someone will notice" is not a cost control.** A task
> that hangs rather than fails holds compute open indefinitely.

---

## 13. Test strategy

| Level | Scope | Gate |
|---|---|---|
| **Unit** | Transformation logic, schema validation, configuration parsing | Every commit |
| **Contract** | Producer payload conforms to §6.1; consumer schema matches producer contract | Every commit |
| **Integration** | Layer-to-layer transitions against a real engine with fixture data | Every commit |
| **Data quality** | Expectation rules fire correctly; quarantine routing verified | Every run |
| **Reconciliation** | DQ-06 arithmetic holds | Every run |
| **Security** | SR-04 — masking verified from a standard-privilege perspective | Every deploy |
| **Performance** | NFR-03 throughput at design load | Pre-production |
| **DR** | Recovery from a deleted position; §11.1 scenarios | Pre-production, then periodically |
| **UAT** | Fraud analysts confirm alert usefulness and false-positive tolerance | Pre-go-live |

| Ref | Requirement |
|---|---|
| TS-01 | Tests shall run in CI on every change, with no manual step. |
| TS-02 | A schema-drift test shall fail the build when the producer payload and the consumer's expected schema diverge. |
| TS-03 | Security verification (SR-04) shall gate deployment. |
| TS-04 | The DR procedure shall be tested, not merely documented. |

> **TS-02 is the highest-value test in the strategy, and it proved so on its
> first run.** The producer and the consumer's parsing schema are two
> definitions of the same contract in different files, and they drift silently —
> the pipeline stays green while the new field is quietly dropped. A test
> asserting the two agree converts a production data-loss incident into a failed
> build.
>
> Implemented, it immediately found three producer guarantees no consumer rule
> enforced (§9.4). All three were satisfied in production at the time, so no
> dashboard, alert or row count would ever have surfaced them.

> **Where the tests run, and what that proves.** Transformation and contract
> tests execute against local Spark — on a laptop and on a CI runner — because
> `from_json`, null handling and SQL predicate evaluation behave identically
> there and on the platform. They gate every commit at zero cost.
>
> They do **not** cover anything platform-specific: Unity Catalog grants,
> Lakeflow expectation accounting, liquid clustering, real Kafka connectivity.
> Those need a workspace and belong in a post-deploy smoke test.
>
> The risk this split creates is named rather than hidden: the quality rules
> exist as `@dp.expect_or_drop` decorators the platform enforces *and* as
> predicates the tests execute. Two expressions of one policy drift.
> `test_contract_alignment.py` asserts they agree, which is what makes local
> testing evidence about production rather than about a parallel copy of it.
>
> **TS-04 states the obvious because the obvious is routinely skipped.** An
> untested DR procedure is a document, and documents do not restore service.

---

## 14. Assumptions, dependencies and risks

### 14.1 Assumptions

| Ref | Assumption | If false |
|---|---|---|
| ASM-01 | Source retention ≥ maximum tolerable outage | RPO = zero unachievable; NFR-06 fails |
| ASM-02 | `transaction_id` is globally unique | Deduplication (FR-11) unsound |
| ASM-03 | Cursor column is updated on **every** source write path | SCD2 history silently incomplete |
| ASM-04 | Transaction volume within design load | NFR-03 re-baselined |
| ASM-05 | Event timestamps are source-generated and monotonic within tolerance | Watermarking unsound; §5.3 invalid |
| ASM-06 | Customer limits are maintained and current | FR-13 produces false positives |

> **ASM-05 is the quiet one.** Watermarking assumes event time is broadly
> well-behaved. A producer with a wrong clock, or one stamping *processing* time
> rather than *event* time, breaks every windowed computation in §4.3 —
> and does so without any error, by producing plausible wrong answers.

### 14.2 Dependencies

| Ref | Dependency | Owner |
|---|---|---|
| DEP-01 | Kafka topic provisioned with contracted retention | Payments Platform |
| DEP-02 | CDC-enabled Postgres access | Core Banking DBA |
| DEP-03 | Unity Catalog metastore and secret scopes | Platform Engineering |
| DEP-04 | Watchlist delivery to governed storage | Fraud Operations |
| DEP-05 | PCI scope determination | Compliance / QSA |
| DEP-06 | GDPR retention and erasure ruling | DPO |
| DEP-07 | Fraud Ops mailbox for alert delivery | Fraud Operations |

### 14.3 Risk register

| Ref | Risk | Impact | Likelihood | Mitigation | Owner |
|---|---|---|---|---|---|
| RISK-01 | No enforced schema registry; contract is consumer-validated only | High | Medium | Contract terms (§6.1); drift test TS-02; quarantine | Lead DE |
| RISK-02 | Unannounced upstream schema change | High | Medium | 90-day clause; quarantine + alert | Payments Platform |
| RISK-03 | Scope creep toward inline blocking (§3.3) | High | Medium | Explicit exclusion; FUT-02 | Sponsor |
| RISK-04 | ASM-03 unverified — cursor may miss bulk updates | High | **Unknown** | **Open action: DBA confirmation required** | Core Banking DBA |
| RISK-05 | DP-03 erasure vs FR-02 immutability unreconciled | High | High | **Open at baseline** — DPO ruling required | DPO |
| RISK-06 | Single-region deployment | Medium | Low | Accepted by sponsor; 24 h RTO | Sponsor |
| RISK-07 | Alert fatigue from false positives degrades response | High | Medium | Per-customer thresholds (FR-13); UAT tuning | Fraud Ops |
| RISK-08 | Late watchlist entries missed (§5.3) | Medium | **Certain** | **Accepted** via CR-004; FUT-03 | Fraud Ops |

> **RISK-08 has likelihood "Certain" deliberately.** It is not a risk of
> something possibly going wrong; it is a known, accepted, permanent limitation
> of the chosen design. Recording it as a risk keeps it visible to anyone who
> later asks why a transaction did not alert.

---

## 15. Out of scope and future phases

### 15.1 Explicitly out of scope

| Ref | Excluded | Why |
|---|---|---|
| OOS-01 | Inline transaction blocking | Wrong latency class and wrong availability profile (§3.3) |
| OOS-02 | ML-based fraud scoring | Requires labelled outcome data this platform will produce; premature |
| OOS-03 | Case management workflow | Existing tool; integration only |
| OOS-04 | Cardholder-facing notification | Owned by Customer Communications |
| OOS-05 | Historical backfill beyond source retention | Data does not exist to load |
| OOS-06 | Multi-region active-active | Cost not justified at current risk appetite (RISK-06) |

### 15.2 Future phases

| Ref | Phase | Depends on |
|---|---|---|
| FUT-01 | Enforced schema registry with producer-side validation | RISK-01 |
| FUT-02 | Real-time scoring service for Payments Platform to call inline | Labelled outcomes; owned by Payments |
| FUT-03 | **Batch retrospective watchlist reconciliation** | CR-004 / RISK-08 |
| FUT-04 | Velocity and behavioural detection rules | Analyst feedback |
| FUT-05 | Analyst feedback loop capturing alert outcomes | Case management integration |
| FUT-06 | **True end-to-end SLA measurement** — event time to alert time, per row | §10.4 |

> **FUT-03 is the designed remedy for the accepted limitation in §5.3.** The
> streaming join handles the timely case; a periodic batch reconciliation
> re-examines historical transactions against the current watchlist and catches
> the late additions the watermark excluded. Splitting the problem this way is
> the standard resolution — it does not require an unbounded streaming state.

---

## 16. Approval and sign-off

Baselining this document authorises the build. Changes after baseline require a
change request (see revision history).

| Role | Name | Approves | Date | Status |
|---|---|---|---|---|
| Business Sponsor — Head of Fraud Operations | | Scope, business drivers, residual risk acceptance | | ☐ |
| Business Analyst | | Functional requirements traceable to BRD-001 | | ☐ |
| Data Architect | | Target architecture, layer rules | | ☐ |
| **Lead Data Engineer** | **Taha Furkhan** | **Technical design, NFRs, author** | 2026-08-11 | ☑ |
| Compliance | | §8 PCI scope design — **veto held** | | ☐ |
| Data Protection Officer | | §8.5 GDPR — **veto held; RISK-05 open** | | ☐ |
| InfoSec | | Access control, secret management | | ☐ |
| SRE / Data Operations | | §10, §11 operability and runbook | | ☐ |

**Open items blocking full sign-off:**

1. **RISK-04** — DBA confirmation that the cursor column is written on every update path
2. **RISK-05** — DPO ruling on erasure vs. immutable raw retention
3. **DEP-05** — QSA scope determination

> These are open at baseline. Build proceeds because none of them changes the
> architecture; each changes a policy parameter within it. Had RISK-05 required
> raw payloads *not* to be retained, the architecture would have changed
> materially and the build would have waited.

---

## 17. Appendix A — Requirements traceability matrix

| Business driver | Functional | Non-functional | Verified by |
|---|---|---|---|
| BD-01 Reduce exposure window | FR-01, FR-12, FR-13, FR-16 | NFR-01, NFR-04 | Latency measurement; UAT |
| BD-02 Reduce manual assembly | FR-14, FR-17, FR-18, FR-19 | NFR-02 | UAT |
| BD-03 Regulatory traceability | FR-02, FR-08, FR-09 | NFR-06, NFR-08 | Reconciliation (DQ-06); DR test |
| BD-04 Contain PCI scope | — | SR-01…SR-06 | Security verification (SR-04, TS-03) |

**Requirements with no verification method are not requirements.** Every row
above terminates in a test or a measurement.

---

## 18. Appendix B — Build authorship statement

This document is written in the form a financial institution would use, with
the stakeholder roles, RACI and sign-off structure such a programme carries.
That structure is genuine and the requirements are real, but the authorship is
not distributed: **this platform was designed and built end-to-end by one
engineer.** The roles in §2 describe who *would* own each decision in a
production organisation, and the RACI is written as it would truly fall.

This appendix exists because the distinction matters, and because three
decisions in this document are ones a single engineer should not legitimately
make alone:

| Decision | § | Who genuinely owns it |
|---|---|---|
| PCI-DSS scope determination | 8.2 | A Qualified Security Assessor. Engineering submits a design; it does not certify. |
| Retention vs. erasure reconciliation | 8.5 / RISK-05 | The DPO. This is a legal ruling, not a technical trade-off. |
| Schema evolution notice periods | 6.1 | The producing team. A contract with one signatory is not a contract. |

Each is marked in the body as submitted-for-approval rather than decided, and
each appears in §16 as an open item blocking full sign-off — which is exactly
where they would sit in a real programme awaiting those approvals.

Where the document records something as *verified in build* — the merchant
duplicate ratio in §6.4, the pipeline execution time in §5.2 — that is measured
from the running system, not estimated.

