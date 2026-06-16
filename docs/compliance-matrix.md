# Sovereign Sentinel — DORA/FINMA Compliance Matrix

**Document Classification:** INTERNAL — INFRASTRUCTURE CONFIDENTIAL
**Version:** 1.2.4
**Last Updated:** Maintained via CI on every merge to `main`

This matrix provides a direct traceability mapping from DORA and FINMA regulatory requirements to specific architectural decisions, implementation components, and verifiable evidence artifacts within the Sovereign Sentinel pipeline.

---

## DORA (EU Regulation 2022/2554)

### Article 9 — ICT Risk Management: Protection & Prevention

| Requirement | Architectural Implementation | Component | Evidence Artifact |
|---|---|---|---|
| 9.2(a) — Identify, classify, and document ICT assets | All workloads labelled with `sentinel.io/data-classification` | `k8s/namespaces/namespaces.yaml` | Namespace YAML labels |
| 9.2(b) — Protect against unauthorized access | Default-deny `NetworkPolicy` across all namespaces; Istio mTLS STRICT mode | `k8s/network-policies/` | Network policy manifests; `istioctl x describe` output |
| 9.2(c) — Detect anomalous activities | Falco DaemonSet with Sentinel-specific runtime rules; Prometheus alerting | `k8s/security/falco/falco-rules.yaml` | Falco alert logs; Prometheus `FalcoSecurityViolation` alert |
| 9.2(d) — Respond and recover | PodDisruptionBudgets; Kafka ISR; checkpoint recovery | `k8s/processing/persistence-consumer.yaml` | DR test log; RTO metrics |
| 9.2(e) — Backup and recovery procedures | PostgreSQL WAL archival to S3; daily base backup; Kafka 7-day retention | `k8s/persistence/postgresql-cluster.yaml` | CloudNativePG backup status; Kafka retention config |
| 9.4 — Encryption in transit | TLS 1.3 on all Kafka listeners; Vault-managed certificates; mTLS mesh | `k8s/streaming/kafka-cluster.yaml`; CertManager | `istioctl proxy-status` showing MUTUAL_TLS |
| 9.4 — Encryption at rest | PostgreSQL storage class with volume encryption; Vault-encrypted secrets | CloudNativePG PVC configuration; Vault | Storage class encryption annotations |
| 9.5 — ICT asset inventory | `k8s/image-manifest.yaml` with SHA256-pinned images | OPA Gatekeeper constraint | Gatekeeper `SentinelRequireImageDigest` violation log (must be empty) |

### Article 10 — ICT Anomaly Detection

| Requirement | Architectural Implementation | Component | Evidence Artifact |
|---|---|---|---|
| 10.1 — Mechanisms to detect anomalous ICT activities | Prometheus alerting rules for lag, latency, ingestion drop, and spread anomalies | `k8s/observability/prometheus.yaml` (PrometheusRule) | Alertmanager alert history; alert resolution timestamps |
| 10.2 — Multiple layers of control | Falco (runtime) + Prometheus (metrics) + Gatekeeper (admission) + Istio (network) | Cross-cutting security components | Security layer independence audit |
| 10.3 — Alert notifications | PagerDuty webhook integration via Alertmanager | `k8s/observability/prometheus.yaml` | Alertmanager notification log |
| 10.4 — Anomalous data patterns | Z-score anomaly detection on FX spread in Spark aggregation job | `spark-jobs/aggregation/ForexAggregationJob.scala` | `forex-audit-events` topic records with `SPREAD_ANOMALY` type |

### Article 11 — ICT Business Continuity

| Requirement | RTO Target | RPO Target | Architectural Mechanism | Evidence |
|---|---|---|---|---|
| 11.1 — BCP for critical ICT functions | < 30 min (full cluster) | < 30s | Multi-AZ pod scheduling; cross-region failover plan | Annual DR test results |
| 11.2 — RTO/RPO for Kafka broker failure | < 30s | 0 | RF=3, ISR=2, `unclean.leader.election=false`, controller election | Monthly chaos test: `scripts/chaos/kafka-broker-kill.sh` |
| 11.2 — RTO/RPO for Spark driver failure | < 60s | 0 | Spark checkpoint recovery on pod restart | Quarterly DR drill results |
| 11.2 — RTO/RPO for PostgreSQL primary failure | < 45s | 0 | CloudNativePG synchronous replication; Patroni-style failover | Monthly failover test results |
| 11.3 — Backup testing | Monthly | — | Automated CloudNativePG backup restore verification | `docs/dr-test-log.md` |
| 11.4 — Recovery communication | < 4h | — | PagerDuty → CISO notification workflow | Incident response runbook |

### Article 17 — ICT Incident Classification & Reporting

| Requirement | Architectural Implementation | Component | Evidence Artifact |
|---|---|---|---|
| 17.1 — Classify ICT incidents | Prometheus alert severity labels (critical, warning, page); DORA compliance labels | `k8s/observability/prometheus.yaml` | Alert label taxonomy |
| 17.2 — Log all ICT incidents | `forex-audit-events` topic; `audit.event_log` table with rejection reasons | `spark-jobs/validation/ForexValidationJob.scala` | Audit topic message count; PostgreSQL row count |
| 17.3 — Regulatory notification timeline | Alertmanager integration for major incidents; CISO escalation runbook | `docs/runbooks/dora-incident-classification.md` | Notification timestamp logs |
| 17.4 — Incident root cause analysis | Comprehensive structured logging; Kafka offset traceability | All pipeline components | Log aggregation (Loki/EFK); Kafka offset replay capability |

### Article 25 — ICT Third-Party Risk Management

| Requirement | Architectural Implementation | Component | Evidence Artifact |
|---|---|---|---|
| 25.1 — Register critical ICT third parties | Dependency manifest in `README.md` with vendor risk tiers | `README.md` §14 | Dependency manifest review; annual vendor assessment |
| 25.2 — Monitor critical third-party services | Prometheus metrics on all third-party components; SLA alerting | `k8s/observability/` | Uptime SLA metrics per component |
| 25.3 — Contractual requirements | All critical vendors on Apache 2.0 / OSI licenses; BSL-1.1 review for Vault | `README.md` §14 | Legal review sign-off |
| 25.4 — Exit strategy | All components replaceable (Kafka → Pulsar, Spark → Flink, etc.) | Architecture Decision Records | `docs/architecture-decision-records/ADR-003-vendor-portability.md` |

---

## FINMA Circular 2023/1 — Operational Risks

### Rz 53–60: Data Integrity for Trading Systems

| FINMA Requirement | Architectural Implementation | Component | Evidence |
|---|---|---|---|
| Rz 53 — Every mutation traceable to principal and timestamp | Kafka producer mTLS CN stored as `event_source_principal` in audit records; dual timestamps (source + pipeline) | `postgres/migrations/V001__initial_schema.sql` | `audit.event_log.event_source_principal` column; `pipeline_ingestion_timestamp_utc` field |
| Rz 54 — Data integrity during processing | Exactly-once semantics: idempotent Kafka producer + Spark checkpoint + transactional outbox | `spark-jobs/validation/ForexValidationJob.scala` | Spark checkpoint directory; Kafka transactional producer config |
| Rz 55 — Tamper-evident audit trail | SHA-256 hash chain in `audit.event_log`; `BEFORE UPDATE OR DELETE` trigger prevents mutation | `postgres/migrations/V001__initial_schema.sql` | `audit.prevent_audit_mutation()` trigger; hash chain verification script |
| Rz 57 — Data integrity in transmission | Istio mTLS STRICT mode; TLS 1.3 on all Kafka listeners; no plaintext inter-service communication | `k8s/security/`; `k8s/streaming/kafka-cluster.yaml` | `istioctl x describe` MUTUAL_TLS confirmation |
| Rz 58 — Non-repudiation | mTLS client certificates identify all producers; certificate CN logged in audit trail | Vault PKI; `kafka-cluster.yaml` KafkaUser resources | Certificate transparency log; `event_source_principal` in audit records |
| Rz 59 — No silent data loss | Rejected records routed to `forex-audit-events`, never dropped; `failOnDataLoss=true` in Spark | `ForexValidationJob.scala` | Audit topic rejection count vs. ingestion count reconciliation |
| Rz 60 — Sequence integrity | Monotonic `sequence_number` field per liquidity provider; Kafka partition ordering per currency pair | `k8s/streaming/kafka-cluster.yaml` (partition key design) | Gap analysis script in `scripts/audit/` |

### Rz 72–79: Recovery Objectives for Tier-1 Systems

| FINMA Requirement | RTO Target | RPO Target | Implementation | Evidence |
|---|---|---|---|---|
| Rz 72 — Market-critical system classification | Tier-1 designation | — | `sentinel.io/job-criticality: tier-1` annotation on SparkApplication | Manifest annotations |
| Rz 73 — RTO for market-critical systems | < 30 min | < 30s | Multi-AZ deployment; automated failover | Annual cross-region DR test |
| Rz 74 — Testing of recovery procedures | Quarterly | — | Documented DR test cadence in `docs/dr-test-log.md` | Signed DR test reports |
| Rz 75 — Data backup procedures | Daily base + continuous WAL | 0 | CloudNativePG WAL streaming to S3; pitr-capable backups | PITR restore test results |
| Rz 76 — Backup retention | 30 days hot, 10 years cold | — | CloudNativePG 30d retention + S3 lifecycle to Glacier | S3 lifecycle policy |
| Rz 77 — Recovery documentation | — | — | Runbooks in `docs/runbooks/` | Runbook review sign-off |

### Rz 87: System Layer Separation

| FINMA Requirement | Architectural Implementation | Evidence |
|---|---|---|
| Rz 87 — Separation of ingestion, processing, storage layers | Dedicated namespaces per layer; default-deny NetworkPolicy between layers; separate ServiceAccounts with zero cross-namespace RBAC permissions | Namespace YAML; NetworkPolicy manifests; RBAC RoleBindings (namespace-scoped) |

### Rz 94–98: Audit Trail Completeness

| FINMA Requirement | Architectural Implementation | Component | Evidence |
|---|---|---|---|
| Rz 94 — Tamper-evident audit log | SHA-256 hash chain; append-only PostgreSQL trigger; immutable Kafka topic (delete-only cleanup) | `postgres/migrations/V001__initial_schema.sql` | Nightly `scripts/audit/audit-integrity-check.sh` results |
| Rz 95 — 10-year retention | Kafka 30-day retention → S3 archive; PostgreSQL 13-month hot partition → S3 Glacier | `k8s/persistence/postgresql-cluster.yaml`; `kafka-cluster.yaml` | S3 object count; partition archival automation |
| Rz 96 — Retrieval SLA | < 4h for archived records | S3 Glacier Instant Retrieval; runbook for data retrieval | Retrieval test records in `docs/dr-test-log.md` |
| Rz 97 — Audit log completeness | No silent drops (validation rejections logged); anomalies flagged not discarded | `ForexValidationJob.scala`; `ForexAggregationJob.scala` | Reconciliation: `COUNT(forex.ticks.raw) = COUNT(forex.ticks.validated) + COUNT(audit.event_log WHERE event_type='VALIDATION_REJECTION')` |
| Rz 98 — Access control to audit logs | `sentinel_audit` role: SELECT-only on `audit.*`; `sentinel_app`: INSERT-only; no UPDATE/DELETE granted to any role | `postgres/migrations/V001__initial_schema.sql` | PostgreSQL `\z audit.*` privilege inspection |

---

## Compliance Verification Cadence

| Control | Verification Method | Frequency | Owner |
|---|---|---|---|
| Hash chain integrity | `scripts/audit/audit-integrity-check.sh` | Nightly (CronJob) | Platform Engineering |
| RTO/RPO — Kafka | `scripts/chaos/kafka-broker-kill.sh` | Monthly | SRE Team |
| RTO/RPO — PostgreSQL | CloudNativePG failover test | Monthly | SRE Team |
| Full DR (cross-region) | Manual coordinated test | Annual | CTO + SRE |
| Audit log reconciliation | Automated SQL assertion | Daily (CronJob) | Platform Engineering |
| Image digest verification | OPA Gatekeeper + CI Trivy scan | Every deployment | CI/CD Pipeline |
| Falco rule effectiveness | Purple-team exercise | Quarterly | Security Team |
| Vendor risk review | Assessment against dependency manifest | Annual | Technology Risk Officer |
| DORA incident classification drill | Tabletop exercise | Semi-annual | CISO + Platform Engineering |

---

*This matrix is a living document and is reviewed quarterly or upon any architectural change.*
*Regulatory contact: Chief Technology Risk Officer*
*Technical owner: Platform Engineering — Financial Infrastructure Team*
