# Fraud Detection Graph Data Model

This document defines the vertices and edges used in a graph database for modeling a fraud detection system.

---

## Vertices

### 1. User
- **Label:** `user`
- **Properties:**
  - `user_id` (string) - Unique identifier for the user
  - `name` (string) - Full name of the user
  - `email` (string) - Email address
  - `phone` (string) - Phone number (optional)
  - `age` (int) - Age of the user
  - `location` (string) - User's location/city
  - `occupation` (string) - User's occupation (default: "Unknown")
  - `risk_score` (float) - Risk assessment score (default: 0.0)
  - `signup_date` (datetime) - Date when user signed up

---

### 2. Account
- **Label:** `account`
- **Properties:**
  - `account_id` (string) - Unique identifier for the account
  - `type` (string) - Account type (e.g., "checking", "savings")
  - `balance` (float) - Current account balance
  - `status` (string) - Account status (default: "active")
  - `bank_name` (string) - Name of the bank (default: "Demo Bank")
  - `created_date` (datetime) - Date when account was created
  - `fraudFlag` (boolean) - Whether account is flagged as fraudulent (optional)
  - `flagReason` (string) - Reason for fraud flag (optional)
  - `flagTimestamp` (datetime) - When account was flagged (optional)

---

### 3. Transaction
- **Label:** `transaction`
- **Properties:**
  - `transaction_id` (string) - Unique identifier for the transaction
  - `amount` (float) - Transaction amount
  - `currency` (string) - Currency code (e.g., "INR")
  - `timestamp` (datetime) - When the transaction occurred
  - `location` (string) - Geographic location of the transaction
  - `fraud_score` (float) - Fraud risk score for this transaction
  - `type` (string) - Transaction type (e.g., "transfer", "payment", "deposit", "withdrawal")
  
  - `status` (string) - Transaction status (e.g., "completed", "pending")
  - `is_fraud` (boolean) - Whether this transaction is flagged as fraudulent
  - `fraud_type` (string) - Type of fraud if detected (optional)
  - `fraud_scenario` (string) - Fraud scenario classification (optional)

**Note:** For transactions created during data seeding, additional properties may include:
  - `method` (string) - Payment method (default: "transfer")
  - `ip_address` (string) - IP address from which transaction was initiated
  - `location_city` (string) - City where transaction occurred
  - `location_country` (string) - Country where transaction occurred
  - `latitude` (float) - Geographic latitude
  - `longitude` (float) - Geographic longitude

---

## Edges

### 1. OWNS
- **From:** `user`
- **To:** `account`
- **Properties:**
  - `since` (datetime) - Date when ownership relationship started (for seeded data)

**Description:** Represents the ownership relationship between a user and their accounts.

---

### 2. TRANSFERS_TO (Transaction Level)
- **From:** `account`
- **To:** `transaction`
- **Properties:** None

**Description:** Represents that an account initiated/sent a transaction. This edge connects the sender account to the transaction.

---

### 3. TRANSFERS_FROM
- **From:** `transaction`
- **To:** `account`
- **Properties:** None

**Description:** Represents that a transaction transfers money to an account. This edge connects the transaction to the receiver account.

---

### 4. TRANSFERS_TO (Account Level)
- **From:** `account`
- **To:** `account`
- **Properties:**
  - `transaction_id` (string) - Reference to the transaction
  - `amount` (float) - Amount transferred
  - `timestamp` (datetime) - When the transfer occurred
  - `status` (string) - Transfer status
  - `method` (string) - Transfer method

**Description:** Direct transfer relationship between accounts. Used for RT1 fraud detection to identify account-to-account transfer patterns.

---

### 5. flagged_by
- **From:** `transaction`
- **To:** `FraudCheckResult`
- **Properties:** None

**Description:** Links a transaction to its fraud check result. Created when RT1 fraud detection identifies a transaction as suspicious.

### 4. FraudCheckResult
- **Label:** `FraudCheckResult`
- **Properties:**
  - `fraud_score` (float) - Fraud risk score (0-100)
  - `status` (string) - Status of fraud check ("review", "blocked", "cleared")
  - `rule` (string) - Name of the fraud detection rule applied (e.g., "flaggedAccountsRule")
  - `evaluation_timestamp` (datetime) - When the fraud check was performed
  - `reason` (string) - Human-readable reason for the fraud flag
  - `details` (string) - Additional details about the fraud check (JSON string)

---

## Data Flow

### Transaction Model
The transaction model uses a vertex-centric approach:

1. **Sender Account** → **Transaction** (via `TRANSFERS_TO` edge)
2. **Transaction** → **Receiver Account** (via `TRANSFERS_FROM` edge)

This allows for:
- Easy traversal to find all transactions initiated by an account
- Easy traversal to find all transactions received by an account
- Rich transaction properties stored on the transaction vertex
- Support for complex fraud detection patterns

### Query Patterns
- **Find sender of a transaction:** `V(transaction).in('TRANSFERS_TO')`
- **Find receiver of a transaction:** `V(transaction).out('TRANSFERS_FROM')`
- **Find all transactions sent by an account:** `V(account).out('TRANSFERS_TO').has_label('transaction')`
- **Find all transactions received by an account:** `V(account).in('TRANSFERS_FROM')`
- **Find all user's transactions:** `V(user).out('OWNS').both('TRANSFERS_TO', 'TRANSFERS_FROM')`

## RT1 Fraud Detection

### RT1: Transaction to Flagged Account Rule

The RT1 rule checks if a transaction's sender or receiver account has previously transferred money to any flagged account.

**Detection Flow:**
1. Transaction is generated and stored in graph with TRANSFERS_TO/TRANSFERS_FROM edges
2. RT1 check runs immediately after storage
3. System performs 1-hop lookup: `V(account).out('TRANSFERS_TO').has_label('account').has('fraudFlag', true)`
4. If flagged connections found:
   - Calculate fraud score (90-100 based on number of connections)
   - Assign status ("review" or "blocked")
   - Create `FraudCheckResult` vertex
   - Create `flagged_by` edge from transaction to result

**Test Setup:**
1. Flag accounts: `POST /accounts/{account_id}/flag`
2. Create transfer relationships: `POST /accounts/{from_account_id}/transfers-to/{to_account_id}`
3. Generate transactions involving connected accounts
4. View fraud results: `GET /fraud-results` or `GET /transaction/{id}/fraud-results`

