# Fraud Detection PoC Plan (Graph-Driven)

## Overview

This document outlines a hybrid fraud detection approach leveraging Aerospike Graph DB, demonstrating both real-time and batch-based detection. The goal is to showcase the power of graph traversal, pattern recognition, and risk scoring using transaction data.

---

## 🎯 Goals

* Highlight **graph traversal capabilities** for fraud detection
* Split detection into **Realtime (low-latency)** and **Batch (deep pattern mining)**
* Use **graph-native queries** to uncover suspicious patterns

---

## 🔁 Detection Modes

### Realtime Detection

**Objective:** Assess each incoming transaction using:

* 1–2 hop graph lookups
* Cached fraud scores
* Lightweight risk signals

### Batch Detection

**Objective:** Periodic scan of full transaction graph to:

* Identify multi-transaction and multi-hop fraud patterns
* Score accounts based on graph analytics

---

## 🧩 Scenario Categorization

### Real-time Detection (RT) Scenarios
| Scenario ID | Description                          | Graph Use              | Detection Mode | Priority | Notes                              |
| ----------- | ------------------------------------ | ---------------------- | -------------- | -------- | ---------------------------------- |
| RT1         | Transaction to flagged account       | 1-hop lookup           | Realtime       | Phase 1  | Immediate threat detection         |
| RT2         |Flagged device     | 2-hop neighborhood     | Realtime       | Phase 1  | Identify mule rings |
| RT3         | Supernode detection (high-degree)    | Centrality check       | Realtime       | Phase 1  | Alert on highly connected accounts |
| RT4         | High-risk batch score                | Vertex property lookup | Realtime       | Phase 1  | Use batch score inline (Coming Soon) |
| RT5         | Transaction burst                    | Time-based clustering  | Realtime       | Phase 1  | Rapid successive transactions      |
| RT2         | Repeated small ring interactions     | 2-hop neighborhood     | Realtime       | Phase 1  | Identify mule rings (Coming Soon) |
### Batch Detection (BT) Scenarios

#### Phase 1 - High Priority
| Scenario ID | Description                          | Graph Use              | Detection Mode | Notes                              |
| ----------- | ------------------------------------ | ---------------------- | -------------- | ---------------------------------- |
| BT1         | Multiple small credits → large debit | Time window path sum   | Batch          | Pattern A (structuring)            |
| BT2         | Large credit → structured equal debits | Fan-out structure    | Batch          | Pattern B (money distribution)     |
| BT3         | High-frequency mule account transfers | Velocity analysis     | Batch          | Pattern D (rapid money movement)   |
| BT4         | Circular transaction flow            | Cycle detection        | Batch          | Detect circular money flows        |
| BT5         | High amount transactions             | Amount threshold       | Batch          | Unusually high transaction amounts |
| BT6         | New user high activity               | User behavior analysis | Batch          | New users with high activity       |

#### Phase 2 - Medium Priority  
| Scenario ID | Description                          | Graph Use              | Detection Mode | Notes                              |
| ----------- | ------------------------------------ | ---------------------- | -------------- | ---------------------------------- |
| BT7         | Multiple large ATM withdrawals       | Transaction pattern    | Batch          | Pattern C (cash extraction)        |
| BT8         | Salary-like deposits → suspicious transfers | Temporal pattern   | Batch          | Pattern E (account takeover)       |
| BT9         | Dormant account sudden activity      | Temporal + path        | Batch          | Pattern F (account compromise)     |
| BT10        | Shared device transactions           | Device fingerprinting  | Batch          | Same device, different users       |
| BT11        | Cross-location transactions          | Geo-analysis           | Batch          | Transactions across locations      |

#### Phase 3 - Lower Priority
| Scenario ID | Description                          | Graph Use              | Detection Mode | Notes                              |
| ----------- | ------------------------------------ | ---------------------- | -------------- | ---------------------------------- |
| BT12        | International high-risk transfers    | Geo-tagged edges       | Batch          | Pattern G (cross-border)           |
| BT13        | Region-specific fraud (Indian)       | Location-based analysis| Batch          | Pattern H (India-specific regions) |

---

## 🛠 Realtime Detection Plan

### Flow:

1. Receive transaction via API
2. Lookup `fraud_score:{account_id}`
3. Run 1-hop Gremlin query:

```gremlin
g.V(account_id).out('transfers_to').has('flagged', true).limit(1)
```

4. Optionally check:

```gremlin
g.V(account_id).out('transfers_to').groupCount().unfold().count()
```

5. Compute risk score and flag if threshold is crossed

### Fast Signals:

* Connection to known fraudsters
* Abnormally high out-degree
* Cached fraud score from batch

---

## 🗃 Batch Detection Plan

### Flow:

1. Ingest all transactions (past 1–6 hrs)
2. Traverse the graph to:

   * Find multi-hop suspicious paths
   * Detect cycles
   * Identify fan-out / structuring
3. Annotate vertex properties:

   * `fraud_score`
   * `cycle_score`
   * `dormancy_flag`
4. Persist results for realtime lookup

### Example Queries:

**BT4 - Money hop in 3 hops within 1 hour**

```gremlin
g.V().hasLabel('account')
 .repeat(__.outE('transfers_to').has('timestamp', P.gt(cutoff)).inV())
 .times(3)
 .path()
 .limit(100)
```

**BT5 - Cycle detection**

```gremlin
g.V().hasLabel('account')
 .repeat(__.out('transfers_to').simplePath()).emit().times(4)
 .where(__.loops().is(P.gte(3)))
 .path()
```

---

## 📊 Visualization (Optional but Recommended)

* Use D3.js or Vis.js to show:

  * Flagged account neighborhood
  * Suspicious path traversals
  * Heatmap of fraud clusters

---

## 📦 PoC Deliverables

### Backend

* Realtime fraud scoring API (Python)
* Batch fraud scoring job (Python/Cron/Airflow)
* Graph queries integrated via Gremlin

### Data

* Simulated transactions (with fraud seeds)
* Seed graph with users, accounts, and edges

### Frontend (Optional)

* Dashboard: flagged transactions, scores, graphs
* Account drill-down: neighbors, transactions, alerts

---

## ✅ Summary

### Real-time Scenarios (Active)
| Type     | Scenario | Graph Query             | Action           | Status    |
| -------- | -------- | ----------------------- | ---------------- | --------- |
| Realtime | RT1      | 1-hop lookup            | Flag transaction | ✅ Active |
| Realtime | RT3      | Centrality check        | Alert on hubs    | ✅ Active |
| Realtime | RT5      | Time-based clustering   | Detect bursts    | ✅ Active |
| Realtime | RT2      | 2-hop neighborhood      | Warn user        | 🚧 Coming Soon |
| Realtime | RT4      | Vertex property lookup  | Risk assessment  | 🚧 Coming Soon |

### Batch Scenarios (by Priority)
| Priority | Scenario | Graph Query             | Action           | Status    |
| -------- | -------- | ----------------------- | ---------------- | --------- |
| Phase 1  | BT1      | Time window path sum    | Score account    | ✅ Ready |
| Phase 1  | BT2      | Fan-out structure       | Detect distribution | 📋 Planned |
| Phase 1  | BT3      | Velocity analysis       | Flag mule activity | ✅ Ready |
| Phase 1  | BT4      | Cycle detection         | Label fraud ring | 📋 Planned |
| Phase 1  | BT5      | Amount threshold        | Flag high amounts | 📋 Planned |
| Phase 1  | BT6      | User behavior analysis  | Monitor new users | ✅ Ready |
| Phase 2  | BT7-BT11 | Various patterns        | Enhanced detection | 📋 Planned |
| Phase 3  | BT12-BT13| Location-based analysis | Regional patterns | 📋 Planned |

This comprehensive plan demonstrates how graph DB enables both **responsive real-time fraud detection** and **deep batch pattern mining**, providing a complete fraud detection ecosystem for modern financial systems.
