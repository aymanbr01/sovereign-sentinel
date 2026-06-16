# The Sovereign Sentinel
### Real-Time Forex Trading Data Pipeline — Audit-Ready for Tier-1 Financial Institutions

[![DORA Compliant](https://img.shields.io/badge/DORA-Compliant-0052CC?style=flat-square)](https://www.digital-operational-resilience-act.com/)
[![FINMA Aligned](https://img.shields.io/badge/FINMA-Aligned-003366?style=flat-square)](https://www.finma.ch/en/)
[![CKS Hardened](https://img.shields.io/badge/Kubernetes-CKS%20Hardened-326CE5?style=flat-square&logo=kubernetes)](https://training.linuxfoundation.org/certification/certified-kubernetes-security-specialist/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.x-231F20?style=flat-square&logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?style=flat-square&logo=apachespark)](https://spark.apache.org/)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Regulatory Context](#2-regulatory-context)
3. [System Architecture](#3-system-architecture)
4. [Data Flow: Ingestion to Storage](#4-data-flow-ingestion-to-storage)
5. [Zero Data Loss Guarantees](#5-zero-data-loss-guarantees)
6. [High-Velocity Stream Handling](#6-high-velocity-stream-handling)
7. [Security Architecture](#7-security-architecture)
8. [Observability & Audit Trail](#8-observability--audit-trail)
9. [Disaster Recovery & RTO/RPO Targets](#9-disaster-recovery--rtorpo-targets)
10. [Repository Structure](#10-repository-structure)
11. [Deployment Prerequisites](#11-deployment-prerequisites)
12. [Deployment Procedure](#12-deployment-procedure)
13. [Operational Runbook Reference](#13-operational-runbook-reference)
14. [Dependency Manifest](#14-dependency-manifest)

---

## 1. Executive Summary

The **Sovereign Sentinel** is a production-grade, cloud-native data pipeline engineered to ingest, process, persist, and audit high-velocity Foreign Exchange (Forex) tick data in real time. The system is designed from the ground up to satisfy the operational resilience mandates of the **Digital Operational Resilience Act (DORA)** (EU Regulation 2022/2554) and the **Swiss Financial Market Supervisory Authority (FINMA)** Circular 2023/1 on operational risks.

This is not a best-effort prototype. Every architectural decision — from Kafka's replication topology to Kubernetes Pod Security Admission configuration — has a traceable compliance rationale documented in the [DORA/FINMA Compliance Matrix](./docs/compliance-matrix.md).

**Core Design Invariants:**

| Invariant | Mechanism |
|---|---|
| Zero data loss under single-node failure | Kafka `min.insync.replicas=2` + `acks=all` producer config |
| Zero data loss under network partition | Kafka partition leadership re-election + consumer offset commits post-persistence |
| Immutable audit log | Kafka topic with `cleanup.policy=compact,delete` + Apache Iceberg cold storage (append-only, snapshot-isolated, time-travel enabled) |
| Sub-second observability | Prometheus scrape interval `5s` on all critical financial processing components |
| Least-privilege runtime | CKS-compliant RBAC + seccomp/AppArmor profiles on all workload pods |
| Financial precision contract | IEEE 754 `double` at transport layer; cast to `Decimal(18,8)` on persistence into ClickHouse and Iceberg storage tiers |
| Cryptographic integrity | TLS 1.3 enforced on all inter-service communication via Istio mTLS |

---

## 2. Regulatory Context

### 2.1 DORA (EU 2022/2554)

The Digital Operational Resilience Act mandates that financial entities within EU jurisdiction establish, maintain, and test ICT risk management frameworks with specific requirements for:

- **Article 9** — ICT Risk Management: Protection and prevention controls with documented detection capabilities.
- **Article 10** — Detection of anomalous ICT activities and operational incidents.
- **Article 11** — ICT Business Continuity with defined RTO/RPO targets for critical functions.
- **Article 17** — ICT-related incident classification, logging, and regulatory notification within prescribed timeframes.
- **Article 25** — ICT Third-Party risk management with continuous monitoring of critical dependencies.

### 2.2 FINMA Circular 2023/1

FINMA's updated Circular on Operational Risks for Swiss financial institutions adds the following specific controls relevant to this architecture:

- **Rz 53–60**: Data integrity requirements for trading systems — every mutation must be traceable to a principal and timestamp.
- **Rz 72–79**: Recovery time objectives for market-critical systems classified as Tier-1.
- **Rz 87**: Requirements for system-level separation between data ingestion, processing, and storage layers.
- **Rz 94–98**: Audit trail completeness — logs must be tamper-evident and retained for a minimum of 10 years for trading data.

The Sovereign Sentinel architecture maps directly to each of these requirements. See [docs/compliance-matrix.md](./docs/compliance-matrix.md) for the full traceability matrix.

---

## 3. System Architecture

### 3.1 High-Level Topology

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                          KUBERNETES CLUSTER (CKS-HARDENED)                           │
│                                                                                      │
│  ┌─────────────────────┐    ┌────────────────────────────────────────────────────┐  │
│  │   INGESTION LAYER   │    │                 PROCESSING LAYER                   │  │
│  │                     │    │   (State: RocksDB + S3 Checkpoints)                │  │
│  │  ┌───────────────┐  │    │  ┌───────────────────┐   ┌───────────────────┐    │  │
│  │  │  FX Source    │  │    │  │  Spark Structured │   │  Spark Streaming  │    │  │
│  │  │  Connector    │──┼────┼─▶│  Streaming Job    │──▶│  Aggregation Job  │    │  │
│  │  │               │  │    │  │  (Validation &    │   │  (OHLCV Calc,     │    │  │
│  │  │  ┌──────────┐ │  │    │  │   Enrichment)     │   │   Anomaly Det.)   │    │  │
│  │  │  │ Confluent│ │  │    │  └────────┬──────────┘   └────────┬──────────┘    │  │
│  │  │  │ Schema   │ │  │    └───────────┼───────────────────────┼───────────────┘  │
│  │  │  │ Registry │ │  │                │                       │                  │
│  │  │  └──────────┘ │  │                │                       │                  │
│  │  └───────────────┘  │                │                       │                  │
│  └─────────────────────┘                │                       │                  │
│                                         │                       │                  │
│  ┌──────────────────────────────────────▼───────────────────────▼──────────────┐   │
│  │              EVENT STREAMING LAYER (Strimzi / Kafka)                        │   │
│  │  Topics:                                                                    │   │
│  │  • forex.ticks.raw            • forex.ticks.poisoned (DLQ — schema errors) │   │
│  │  • forex.ticks.validated      • forex.audit.events                         │   │
│  │  • forex.ohlcv.aggregated                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                           │
│  ┌──────────────────────────────────────▼──────────────────────────────────────┐   │
│  │                   TIERED STORAGE LAYER (Trident Architecture)               │   │
│  │                                                                             │   │
│  │  ┌─────────────────────────┐      ┌──────────────────────────────────────┐ │   │
│  │  │  HOT OLAP (Sub-ms)      │      │  COLD STORAGE (Immutable)            │ │   │
│  │  │  ClickHouse Cluster     │      │  Apache Iceberg on S3/MinIO          │ │   │
│  │  │  (MergeTree Engine)     │      │  (Parquet + Zstd, Time-Travel)       │ │   │
│  │  └────────────▲────────────┘      └─────────────────▲────────────────────┘ │   │
│  └───────────────┼───────────────────────────────────── ┼────────────────────┘   │
│                  │                                       │                         │
│  ┌───────────────▼───────────────────────────────────────▼──────────────────────┐  │
│  │               GOVERNANCE & OBSERVABILITY LAYER                               │  │
│  │  Grafana Dashboards (Live UI)  |  OpenLineage + Marquez (Data Lineage)       │  │
│  │  Prometheus + Alertmanager     |  DORA-Compliant Provenance Graph            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                        SECURITY PLANE                                        │   │
│  │  HashiCorp Vault (Secrets)  |  Istio mTLS (Service Mesh)                    │   │
│  │  OPA Gatekeeper (Policy)    |  Falco (Runtime Threat Detection)              │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Namespace Segmentation

Strict namespace isolation enforces the FINMA Rz 87 separation requirement and enables granular RBAC and NetworkPolicy enforcement.

| Namespace | Workloads | Egress Allowed To |
|---|---|---|
| `sentinel-ingestion` | Kafka Connect workers | `sentinel-streaming` (Kafka brokers) |
| `sentinel-streaming` | Kafka brokers, ZooKeeper/KRaft | `sentinel-ingestion`, `sentinel-processing` |
| `sentinel-processing` | Spark Driver, Spark Executors | `sentinel-streaming`, `sentinel-persistence` |
| `sentinel-persistence` | ClickHouse Cluster & Iceberg/MinIO | `sentinel-processing` |
| `sentinel-observability` | Prometheus, Alertmanager, Grafana | All namespaces (read-only scrape) |
| `sentinel-security` | Vault Agent, Falco, OPA Gatekeeper | All namespaces (policy enforcement) |

---

## 4. Data Flow: Ingestion to Storage

The pipeline is structured as a sequence of deterministic, idempotent processing stages. Each stage boundary is mediated by a Kafka topic, providing the durability and replay capability required for both operational resilience and regulatory audit.

### Stage 1: Source Ingestion (Kafka Connect)

**Component:** Kafka Connect cluster (`sentinel-ingestion` namespace)
**Input:** FX tick feed (WebSocket/FIX protocol from upstream liquidity providers)
**Output:** `forex.ticks.raw` Kafka topic

The ingestion layer uses a custom Kafka Source Connector deployed within Kafka Connect. Each FX tick message is:

1. Validated for structural integrity at the connector level via Confluent Schema Registry (BACKWARD_TRANSITIVE compatibility enforced). Records that fail Avro schema validation are never written to `forex.ticks.raw` — they are routed directly to the `forex.ticks.poisoned` dead-letter queue (DLQ) with a structured rejection envelope containing the raw payload, violation code, and connector timestamp, ensuring zero silent data loss at the ingestion boundary.
2. Enriched with a `pipeline_ingestion_timestamp_utc` field (connector-side, monotonic) to distinguish feed timestamps from arrival timestamps — a FINMA Rz 53 compliance requirement for timestamp provenance.
3. Assigned a deterministic Kafka message key of `{currency_pair}:{timestamp_epoch_micros}` to ensure ordered partitioning per currency pair.
4. Written to `forex.ticks.raw` with producer `acks=all` and `enable.idempotence=true`.

**Delivery guarantee at this stage:** At-least-once delivery from source, exactly-once writes to Kafka (idempotent producer + transactional API).

### Stage 2: Validation & Enrichment (Spark Structured Streaming — Job 1)

**Component:** Spark Structured Streaming application (`sentinel-processing` namespace)
**Input:** `forex.ticks.raw`
**Output:** `forex.ticks.validated` | `forex.audit.events` (for rejected records)

The first Spark job performs stateless, record-level validation and enrichment:

- **Schema validation**: Enforce Avro schema contract. Records failing schema validation are routed to `forex.audit.events` with rejection code `SCHEMA_VIOLATION` — they are never silently dropped.
- **Business rule validation**: Price sanity checks (bid ≤ ask, spread within configurable threshold per pair), timestamp monotonicity per partition.
- **PII/Sensitivity tagging**: Currency pair metadata enrichment from a periodically-refreshed reference dataset stored in ClickHouse (`trading.currency_pair_ref`).
- **Watermarking**: A 10-second event-time watermark is applied to handle late-arriving ticks from geographically dispersed liquidity providers. Late records beyond the watermark are still persisted to the audit topic but excluded from aggregation windows — they are never silently discarded.

Checkpointing is written to a persistent volume (`sentinel-checkpoint-pvc`, `ReadWriteOnce`, backed by a replicated storage class) at the end of each micro-batch, ensuring exactly-once processing semantics via Spark's write-ahead log mechanism.

**Delivery guarantee at this stage:** Exactly-once processing via Spark checkpoint + Kafka transactional sink.

### Stage 3: Aggregation & Anomaly Detection (Spark Structured Streaming — Job 2)

**Component:** Spark Structured Streaming application (`sentinel-processing` namespace)
**Input:** `forex.ticks.validated`
**Output:** `forex.ohlcv.aggregated` | `forex.audit.events` (for anomaly flags)

The second Spark job performs stateful stream aggregation:

- **OHLCV Calculation**: Tumbling windows of 1s, 10s, 1m, 5m, and 1h over event time. Each window produces an OHLCV record (Open/High/Low/Close/Volume) per currency pair per interval.
- **Anomaly Detection**: Z-score based statistical outlier detection on spread and mid-price velocity. Anomalous records are flagged and emitted to `forex.audit.events` with anomaly metadata — they are simultaneously included in the OHLCV output (flagged) so that downstream consumers can make their own exclusion decisions. This satisfies the DORA Article 10 requirement for anomalous activity detection without data loss.
- **State Store**: RocksDB state backend for low-GC-pressure stateful aggregations, with state snapshots to the same persistent checkpoint volume.

### Stage 4: Tiered Storage Persistence (ClickHouse & Apache Iceberg)

**Component:** Dedicated Kafka Consumer application (JVM, `sentinel-processing` namespace)
**Input:** `forex.ticks.validated`, `forex.ohlcv.aggregated`, `forex.audit.events`
**Output:** ClickHouse (hot OLAP tier) + Apache Iceberg on S3/MinIO (cold immutable tier)

The persistence layer implements a **dual-sink, tiered storage architecture** — the Trident model:

**Hot Tier — ClickHouse (MergeTree Engine):**
1. Records are consumed from Kafka in batches and written to ClickHouse via the native `clickhouse-client` JDBC driver with `async_insert=1` for sub-ms ingestion latency.
2. The `trading.forex_ticks` table uses a `ReplicatedMergeTree` engine with `ORDER BY (currency_pair, source_timestamp_micros)` for optimal time-series query performance.
3. Idempotency is enforced via ClickHouse's built-in deduplication window (`replicated_deduplication_window=100`). Duplicate inserts within the window are silently discarded; a conflict counter is emitted as `sentinel_persistence_duplicate_writes_total`.
4. Retention policy: 30 days of hot data. TTL expressions automatically move partitions to the cold tier on schedule.

**Cold Tier — Apache Iceberg (Parquet + Zstd on S3/MinIO):**
1. An Iceberg sink connector writes immutable, append-only Parquet files to S3-compatible object storage (MinIO in-cluster, or AWS S3 in cloud deployments).
2. Zstd compression achieves ~90% storage reduction versus raw JSON. Files are partitioned by `(currency_pair, date)` for efficient predicate pushdown.
3. Iceberg's snapshot isolation and time-travel capability (`SELECT ... AS OF TIMESTAMP`) provides auditable, tamper-evident historical access — a DORA Article 11 compliance requirement for immutable audit trails.
4. Schema evolution is governed by the same Confluent Schema Registry contract used at ingestion. Iceberg's hidden partitioning absorbs schema additions without table rewrites.

**Data Lineage — OpenLineage + Marquez:**
Every batch write to both tiers emits an OpenLineage `RunEvent` to the Marquez lineage server, recording the Kafka source offset range, the target dataset, and the processing job version. This creates a DORA-compliant, end-to-end provenance graph traceable from raw WebSocket tick to persisted Iceberg snapshot.

This design guarantees: **a record confirmed as written to ClickHouse has a corresponding committed Kafka offset; a committed Kafka offset guarantees a record in both the hot and cold tiers.** There is no window in which a record can be in one system but not the other.

---

## 4.5 Elite-Grade Data Contracts (Avro Schema)

All data crossing a Kafka topic boundary is serialized using Apache Avro and governed by Confluent Schema Registry with `BACKWARD_TRANSITIVE` compatibility enforced. This means every consumer can deserialize every historical message ever written to the topic — schema evolution never breaks downstream systems.

The canonical schema for the ingestion boundary (`forex.ticks.raw`) is:

```json
{
  "type": "record",
  "name": "FxTick",
  "namespace": "com.sentinel.forex",
  "doc": "Immutable, auditable, tamper-evident record of a single FX best-bid/ask tick.",
  "fields": [
    { "name": "event_id",                         "type": "string",  "doc": "UUID v4. Globally unique per tick." },
    { "name": "currency_pair",                    "type": "string",  "doc": "ISO 4217 pair, e.g. EUR/USD." },
    { "name": "bid_price",                        "type": "double",          "doc": "IEEE 754 double. Cast to Decimal(18,8) on persistence." },
    { "name": "ask_price",                        "type": "double",          "doc": "IEEE 754 double. Cast to Decimal(18,8) on persistence." },
    { "name": "bid_size",                         "type": ["null", "double"], "default": null },
    { "name": "ask_size",                         "type": ["null", "double"], "default": null },
    { "name": "source_timestamp_micros",          "type": "long",    "doc": "Exchange-side event time, epoch microseconds." },
    { "name": "pipeline_ingestion_timestamp_utc", "type": "long",    "doc": "Connector-side arrival time, epoch microseconds. FINMA Rz 53." },
    { "name": "liquidity_provider_id",            "type": "string",  "doc": "Upstream LP identifier, e.g. LP-BINANCE-LIVE." },
    { "name": "venue_id",                         "type": ["null", "string"], "default": null },
    { "name": "sequence_number",                  "type": "long",    "doc": "Monotonically increasing per LP session." }
  ]
}
```

**Schema governance rules enforced at registration time:**
- `BACKWARD_TRANSITIVE` compatibility: all existing consumers remain unbroken on any schema version.
- Required fields may never be removed; optional fields must carry a `null` default.
- Pragmatic precision: Financial values are transported as IEEE 754 `double` for low-latency JVM processing, but are strictly cast to `Decimal(18,8)` upon persistence into the ClickHouse and Iceberg storage tiers to prevent historical rounding errors.
- Every schema change requires a pull request with a compliance sign-off from the Financial Infrastructure Team lead.

---

## 5. Zero Data Loss Guarantees

The zero data loss guarantee is a property of the entire pipeline, not any single component. It requires that every failure mode has a documented recovery path that does not involve data discarding.

### 5.1 Failure Mode Matrix

| Failure Scenario | Detection | Recovery Path | Data Loss |
|---|---|---|---|
| Kafka broker pod failure | Kafka controller election (<30s) | Partition leadership fails over to ISR followers. No consumer disruption for properly configured clients. | **None** |
| Spark executor pod failure | Spark driver detects executor heartbeat timeout | Driver reallocates tasks to surviving executors. Checkpoint ensures no re-processing gap. | **None** |
| Spark driver pod failure | Kubernetes deployment controller | Pod is rescheduled; Spark resumes from last committed checkpoint. At-most 1 micro-batch of reprocessing (idempotent). | **None** |
| Persistence consumer pod failure | Kubernetes deployment controller | Pod rescheduled; resumes from last committed Kafka offset. Uncommitted records reprocessed idempotently. | **None** |
| ClickHouse node failure | ClickHouse Keeper (ZooKeeper-compatible) detects node loss | Replica takes over reads/writes automatically; JDBC connector retries with exponential backoff. | **None** |
| Full AZ failure | Kubernetes node pool health | Multi-AZ pod scheduling ensures workloads survive single-AZ loss. Kafka RF=3 with `replica.assignment` spanning AZs. | **None** |
| Network partition (consumer ↔ Kafka) | Consumer group coordinator timeout | Consumer group rebalance. No offsets committed during partition = records reprocessed. Idempotency layer absorbs duplicates. | **None** |
| Source feed disconnection | Kafka Connect task health metrics | Connect framework automatically restarts failed tasks (max.tasks.restart.delay.ms configured). Gap in ingestion is a feed availability issue, not a pipeline data loss issue. | **Pipeline: None. Feed gap: external.** |

### 5.2 Kafka Durability Configuration

The following topic and broker configurations are the non-negotiable foundation of the durability guarantee:

```properties
# Broker-level (applied to all brokers via ConfigMap)
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
log.retention.hours=168        # 7-day retention for replay capability
log.retention.bytes=-1         # Size-unlimited (retention by time only)
auto.create.topics.enable=false # Prevent accidental topic creation

# Producer configuration (Kafka Connect + Spark Kafka sink)
acks=all
enable.idempotence=true
retries=2147483647
max.in.flight.requests.per.connection=5
delivery.timeout.ms=120000

# Consumer configuration (Spark + Persistence consumer)
isolation.level=read_committed  # Only consume committed transactional records
enable.auto.commit=false        # Manual offset management only
```

**Critical invariant:** `unclean.leader.election.enable=false` is non-negotiable. Enabling unclean leader election for performance gain would void the zero data loss guarantee by allowing an out-of-sync replica to become leader, potentially serving stale or missing records.

---

## 6. High-Velocity Stream Handling

> **Scale context:** At full production scale with all 24 currency pairs and multiple liquidity providers, the pipeline is sized for sustained throughput of 500,000 tick messages per second. The current PoC deployment ingests live data from the Binance public WebSocket feed for demonstration purposes, operating at feed-native rates of 1–50 ticks/second per pair.

The Sovereign Sentinel is sized for **sustained throughput of 500,000 tick messages per second** across all currency pairs, with burst tolerance to 1,200,000 messages/second for up to 90 seconds (covering FX market open volatility events).

### 6.1 Partitioning Strategy

`forex.ticks.raw` is provisioned with **24 partitions** — one partition per major currency pair group (G10 pairs, EM pairs, exotics). This provides:

- **Parallelism**: 24 parallel Spark tasks per micro-batch for validation processing.
- **Ordering**: Strict tick ordering preserved per currency pair within a partition.
- **Scalability**: Partition count can be increased to 48 without reprocessing historical data, as the partition key is deterministic and consumers are pair-aware.

### 6.2 Spark Executor Sizing

```
Job 1 (Validation):    4 executors × (4 vCPU / 8 GiB RAM)  = 16 vCPU / 32 GiB
Job 2 (Aggregation):   6 executors × (4 vCPU / 16 GiB RAM) = 24 vCPU / 96 GiB
                       (higher memory for RocksDB state store)
```

Executor count is managed by Spark's Dynamic Resource Allocation (DRA) with Kubernetes as the resource manager. DRA is bounded by `spark.dynamicAllocation.maxExecutors` to prevent unbounded cluster resource consumption — a required control under DORA Article 9 (resource management).

### 6.3 Kafka Consumer Throughput Tuning

```properties
# Per Spark streaming query
kafka.consumer.fetch.min.bytes=1048576      # 1 MiB minimum fetch
kafka.consumer.fetch.max.wait.ms=500
kafka.consumer.max.partition.fetch.bytes=10485760  # 10 MiB per partition
kafka.consumer.max.poll.records=5000
```

Micro-batch trigger interval is set to `ProcessingTime("500 milliseconds")` — providing sub-second end-to-end latency from ingestion to validated topic availability under normal load.

### 6.4 Back-Pressure Architecture

Back-pressure is implemented at three levels:

1. **Spark level**: `maxOffsetsPerTrigger` limits the maximum records consumed per micro-batch, preventing executor OOM during burst ingestion events.
2. **Kafka level**: Kafka's internal replication and ISR mechanism absorbs producer bursts without data loss — producers block when all ISR replicas are not caught up (enforced by `acks=all`).
3. **Ingestion level**: Kafka Connect's `producer.override.max.block.ms` ensures that if Kafka brokers are saturated, the Connect worker applies back-pressure to the upstream FX source rather than buffering unboundedly in JVM heap.

This three-tier back-pressure design ensures that overload conditions degrade gracefully (increased latency, controlled queuing) rather than catastrophically (OOM crashes, data loss).

---

## 7. Security Architecture

### 7.1 Kubernetes Hardening (CKS Standards)

All workload pods are deployed under the `Restricted` Pod Security Standard (PSS) enforced via Pod Security Admission (`PodSecurity` admission controller). No pod in the system runs with:

- `runAsRoot: true`
- `privileged: true`
- `hostNetwork: true` or `hostPID: true`
- `allowPrivilegeEscalation: true`
- Writable root filesystem (`readOnlyRootFilesystem: false`)

Capability sets are explicitly constrained: `ALL` capabilities dropped; no capabilities added except `NET_BIND_SERVICE` where strictly required (and only on specific containers).

### 7.2 Network Policy Enforcement

`NetworkPolicy` resources enforce a **default-deny-all** posture at the namespace level. Allowlists are applied surgically per workload. No cross-namespace communication is permitted unless explicitly modelled in a `NetworkPolicy` manifest. The manifests are in [k8s/network-policies/](./k8s/network-policies/).

### 7.3 mTLS via Istio Service Mesh

All inter-pod communication is encrypted at the transport layer via Istio's automatic mTLS injection. `PeerAuthentication` policies are set to `STRICT` mode cluster-wide — unauthenticated plaintext connections between Sentinel services are refused at the mesh layer. This satisfies DORA Article 9 (encryption in transit) and FINMA Rz 57 (data integrity during transmission).

### 7.4 Secret Management

All secrets (Kafka credentials, ClickHouse credentials, TLS certificates, FX feed API keys) are managed by **HashiCorp Vault** with the Kubernetes auth backend. The Vault Agent Injector sidecar delivers secrets to pods as in-memory tmpfs-mounted files — secrets are never stored in Kubernetes `Secret` objects, etcd, or container images.

CertManager handles automated TLS certificate issuance and rotation for all internal services with a maximum certificate lifetime of 90 days and automated renewal at 80% of lifetime.

### 7.5 Runtime Threat Detection

Falco is deployed as a DaemonSet with a custom ruleset covering Sentinel-specific threat signatures:

- Unexpected outbound connections from processing pods (potential data exfiltration)
- `exec` calls into running Kafka or Spark JVM pods
- Unexpected file writes to non-tmpfs mounts from Spark executors
- Privilege escalation attempts within any Sentinel namespace

Falco alerts are routed to the Prometheus alerting pipeline with a `severity: critical` label for immediate PagerDuty notification.

### 7.6 Supply Chain Security

All container images are:

- Built from verified base images pinned to immutable SHA256 digests
- Scanned by Trivy at build time (CRITICAL/HIGH CVEs fail the CI pipeline)
- Signed with Cosign and verified by OPA/Gatekeeper admission policy (unsigned images are rejected at admission)
- Stored in a private container registry with immutable tags

The complete image inventory with digest pins is maintained in [k8s/image-manifest.yaml](./k8s/image-manifest.yaml).

---

## 8. Observability & Audit Trail

### 8.1 Prometheus Metrics

The following custom metrics are exposed by Sentinel components and are considered **mandatory operational metrics** for DORA Article 10 compliance:

| Metric | Type | Description | Alert Threshold |
|---|---|---|---|
| `sentinel_tick_ingestion_rate` | Gauge | Ticks/second entering `forex.ticks.raw` | < 10/s for > 30s during market hours |
| `sentinel_kafka_consumer_lag_max` | Gauge | Maximum lag across all consumer groups | > 50,000 messages |
| `sentinel_validation_rejection_rate` | Counter | Records rejected per second by validation | > 0.1% of ingestion rate |
| `sentinel_spark_micro_batch_duration_seconds` | Histogram | Processing duration per micro-batch | p99 > 2s |
| `sentinel_persistence_write_latency_seconds` | Histogram | ClickHouse/Iceberg write latency | p99 > 500ms |
| `sentinel_persistence_duplicate_writes_total` | Counter | Duplicate write attempts absorbed | > 100/min (indicates upstream reprocessing storm) |
| `sentinel_pipeline_end_to_end_latency_seconds` | Histogram | Tick ingestion to DB persistence latency | p99 > 5s |
| `sentinel_audit_events_total` | Counter | Total audit events emitted (by type) | Anomaly type spike |

### 8.2 Audit Trail Architecture

The `forex.audit.events` Kafka topic and the **Apache Iceberg cold storage tier** constitute the **tamper-evident audit trail** required by FINMA Rz 94. Design properties:

- **Append-only**: Iceberg's immutable snapshot model makes it physically impossible to mutate or delete committed data files. Every write creates a new snapshot; no snapshot is ever overwritten.
- **Cryptographic chaining**: Each audit record includes a `prev_record_hash` field (SHA-256 of the previous record's canonical representation), creating a hash chain detectable via integrity check procedures in [scripts/audit-integrity-check.sh](./scripts/audit-integrity-check.sh).
- **10-year retention**: Iceberg partitions by `(event_type, month)` on S3/MinIO with lifecycle policies enforcing immutability for 10 years. Time-travel queries (`SELECT ... AS OF TIMESTAMP`) provide point-in-time retrieval with SLA of < 4 hours. Retention policy documented in [docs/data-retention-policy.md](./docs/data-retention-policy.md).
- **Non-repudiation**: All audit events carry the Kafka producer principal (mTLS client certificate CN) as `event_source_principal` — establishing a cryptographically-verified chain of custody from ingestion to persistence.

---

## 9. Disaster Recovery & RTO/RPO Targets

These targets are commitments against which the system must be tested under DORA Article 11 (ICT Business Continuity).

| Scenario | RTO | RPO | Validation Method |
|---|---|---|---|
| Single Kafka broker failure | < 30 seconds | 0 (zero data loss) | Monthly chaos test: `kubectl delete pod kafka-broker-X` |
| Single Spark executor failure | < 15 seconds | 0 | Monthly chaos test: executor pod kill |
| Spark driver (job restart) | < 60 seconds | 0 (checkpoint recovery) | Quarterly DR drill |
| ClickHouse node failover | < 30 seconds | 0 (ClickHouse Keeper + replica) | Monthly ClickHouse node kill test |
| Full namespace failure | < 5 minutes | 0 | Quarterly full namespace recreation test |
| Full cluster failure (cross-region) | < 30 minutes | < 30 seconds of tick data | Annual DR test with cross-region failover |

DR test results are documented in [docs/dr-test-log.md](./docs/dr-test-log.md) and submitted as evidence in the annual DORA operational resilience assessment.

---

## 10. Repository Structure

```
sovereign-sentinel/
├── README.md                          # This document
├── docs/
│   ├── compliance-matrix.md           # DORA/FINMA compliance traceability
│   ├── architecture-decision-records/ # ADRs for key design decisions
│   ├── dr-test-log.md                 # Disaster recovery test history
│   ├── data-retention-policy.md       # Data lifecycle and retention policy
│   └── threat-model.md                # STRIDE threat model
├── k8s/
│   ├── namespaces/                    # Namespace definitions with labels
│   ├── rbac/                          # ClusterRoles, Roles, Bindings
│   ├── network-policies/              # NetworkPolicy manifests per namespace
│   ├── pod-security/                  # PodSecurityPolicy / PSS configurations
│   ├── ingestion/                     # Kafka Connect deployment manifests
│   ├── streaming/                     # Kafka cluster manifests (Strimzi CRDs)
│   ├── processing/                    # Spark Operator + SparkApplication CRDs
│   ├── persistence/                   # ClickHouse cluster & Iceberg/MinIO manifests
│   ├── observability/                 # Prometheus, Alertmanager, Grafana manifests
│   ├── security/                      # Vault, Falco, OPA Gatekeeper, CertManager
│   └── image-manifest.yaml            # Pinned image digest inventory
├── spark-jobs/
│   ├── validation/                    # Spark Job 1 source (Scala/Python)
│   └── aggregation/                   # Spark Job 2 source (Scala/Python)
├── kafka-connect/
│   └── fx-source-connector/           # Custom connector source
├── scripts/
│   ├── audit-integrity-check.sh       # Hash chain verification
│   ├── dr-test/                       # Automated DR test scripts
│   └── chaos/                         # Chaos engineering scripts (pod kill, AZ failure)
├── helm/
│   └── sovereign-sentinel/            # Umbrella Helm chart
└── ci/
    ├── pipeline.yaml                  # CI/CD pipeline definition
    ├── cosign/                        # Image signing configuration
    └── trivy-policy.yaml              # Vulnerability gate policy
```

---

## 11. Deployment Prerequisites

| Prerequisite | Minimum Version | Notes |
|---|---|---|
| Kubernetes | 1.29 | PSS `Restricted` requires 1.25+; 1.29 for latest security features |
| Helm | 3.14 | Required for umbrella chart deployment |
| kubectl | 1.29 | Must match cluster version ±1 |
| Istio | 1.21 | mTLS strict mode; `ambient` mesh supported |
| Strimzi Operator | 0.40 | Manages Kafka CRDs |
| Spark Operator | 1.4 | Kubeflow Spark Operator for SparkApplication CRDs |
| ClickHouse Operator | 0.23 | Manages ClickHouse cluster CRDs |
| HashiCorp Vault | 1.16 | Kubernetes auth backend required |
| CertManager | 1.14 | Certificate issuance and rotation |
| OPA Gatekeeper | 3.16 | Admission control policy enforcement |
| Falco | 0.38 | DaemonSet runtime security |
| Prometheus Operator | 0.73 | `ServiceMonitor` CRD support |

---

## 12. Deployment Procedure

### 12.1 Cluster Preparation

```bash
# 1. Verify cluster security posture
kubectl get --raw /api/v1 | jq '.serverAddressByClientCIDRs'
kubectl get psp 2>/dev/null && echo "WARNING: PSP enabled — migrate to PSA"

# 2. Apply namespace isolation
kubectl apply -f k8s/namespaces/

# 3. Enforce Pod Security Standards
kubectl label namespace sentinel-processing \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/audit=restricted \
  pod-security.kubernetes.io/warn=restricted

# (Repeat for all sentinel-* namespaces)

# 4. Deploy security infrastructure (Vault, Gatekeeper, Falco, CertManager)
helm upgrade --install sentinel-security ./helm/sovereign-sentinel \
  --namespace sentinel-security \
  --create-namespace \
  -f helm/sovereign-sentinel/values-security.yaml \
  --atomic --timeout 10m

# 5. Verify Vault unsealed and Kubernetes auth configured
vault auth list | grep kubernetes
```

### 12.2 Data Plane Deployment

```bash
# Deploy in dependency order
helm upgrade --install sentinel-streaming ./helm/sovereign-sentinel \
  --namespace sentinel-streaming -f values-streaming.yaml --atomic

helm upgrade --install sentinel-persistence ./helm/sovereign-sentinel \
  --namespace sentinel-persistence -f values-persistence.yaml --atomic

helm upgrade --install sentinel-processing ./helm/sovereign-sentinel \
  --namespace sentinel-processing -f values-processing.yaml --atomic

helm upgrade --install sentinel-ingestion ./helm/sovereign-sentinel \
  --namespace sentinel-ingestion -f values-ingestion.yaml --atomic
```

### 12.3 Post-Deployment Verification

```bash
# Verify zero consumer lag (pipeline is live and processing)
kubectl exec -n sentinel-streaming kafka-broker-0 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --all-groups

# Verify audit trail integrity
./scripts/audit-integrity-check.sh --from "$(date -d '1 hour ago' --iso-8601=seconds)"

# Verify mTLS is enforced (all connections should show MUTUAL_TLS)
istioctl x describe service kafka-broker -n sentinel-streaming
```

---

## 13. Operational Runbook Reference

| Scenario | Runbook |
|---|---|
| Kafka consumer lag growing | [docs/runbooks/kafka-lag.md](./docs/runbooks/kafka-lag.md) |
| Spark job restarting repeatedly | [docs/runbooks/spark-restart-loop.md](./docs/runbooks/spark-restart-loop.md) |
| ClickHouse node failover | [docs/runbooks/clickhouse-failover.md](./docs/runbooks/clickhouse-failover.md) |
| Audit integrity check failure | [docs/runbooks/audit-integrity-breach.md](./docs/runbooks/audit-integrity-breach.md) |
| Falco alert: unexpected egress | [docs/runbooks/falco-egress-alert.md](./docs/runbooks/falco-egress-alert.md) |
| FX source feed disconnection | [docs/runbooks/feed-disconnection.md](./docs/runbooks/feed-disconnection.md) |
| DORA incident notification | [docs/runbooks/dora-incident-classification.md](./docs/runbooks/dora-incident-classification.md) |

---

## 14. Dependency Manifest

All third-party dependencies are pinned to immutable versions. This table constitutes part of the ICT Third-Party Risk register required under DORA Article 25.

| Component | Version | License | Vendor Risk Tier |
|---|---|---|---|
| Apache Kafka | 3.7.1 | Apache 2.0 | Tier 1 (Critical) |
| Apache Spark | 3.5.2 | Apache 2.0 | Tier 1 (Critical) |
| ClickHouse | 24.3.0 | Apache 2.0 | Tier 1 (Critical) |
| Apache Iceberg | 1.5.0 | Apache 2.0 | Tier 1 (Critical) |
| Confluent Schema Registry | 7.6.0 | Confluent Community License | Tier 1 (Critical) |
| OpenLineage (Marquez) | 1.9.0 | Apache 2.0 | Tier 2 |
| Strimzi Operator | 0.40.0 | Apache 2.0 | Tier 2 |
| Spark Operator | 1.4.6 | Apache 2.0 | Tier 2 |
| Prometheus | 2.52.0 | Apache 2.0 | Tier 2 |
| Grafana | 10.4.2 | AGPL 3.0 | Tier 2 |
| HashiCorp Vault | 1.16.2 | BSL 1.1 | Tier 1 (Critical) |
| Istio | 1.21.2 | Apache 2.0 | Tier 1 (Critical) |
| OPA Gatekeeper | 3.16.3 | Apache 2.0 | Tier 2 |
| Falco | 0.38.1 | Apache 2.0 | Tier 2 |
| CertManager | 1.14.5 | Apache 2.0 | Tier 2 |

---

*This document is classified as: **INTERNAL — INFRASTRUCTURE CONFIDENTIAL***
*Last reviewed: per CI pipeline on every merge to `main`*
*Owner: Platform Engineering — Financial Infrastructure Team*
*Regulatory contact: Chief Technology Risk Officer*
