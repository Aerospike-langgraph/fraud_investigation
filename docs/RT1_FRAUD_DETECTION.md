# RT1 Fraud Detection System

This document describes the implementation of the RT1 (Real-time Transaction to Flagged Account) fraud detection system.

## Overview

The RT1 fraud detection system implements **real-time fraud detection** based on the plan.md specification. It detects when a transaction's sender or receiver account has previously transferred money to any flagged account.

## How It Works

### 1. **Transaction Generation**
- Transactions are generated as normal financial transactions
- No fraud identifiers are added during generation
- All transactions start as clean, legitimate transactions

### 2. **RT1 Fraud Detection (Post-Transaction)**
After each transaction is stored in the graph database:

1. **1-hop Graph Lookup**: Check if sender/receiver accounts have `TRANSFERS_TO` edges pointing to flagged accounts
2. **Fraud Scoring**: If flagged connections found, assign fraud score (90-100)
3. **Status Assignment**: 
   - Score ≥95: "blocked"
   - Score <95: "review"
4. **Result Storage**: Create `FraudCheckResult` vertex with evaluation details
5. **Edge Creation**: Link transaction to fraud result via `flagged_by` edge

### 3. **Graph Data Model**

```
[Sender Account] --TRANSFERS_TO--> [Transaction] --TRANSFERS_FROM--> [Receiver Account]
       |                                                                     
       |--TRANSFERS_TO--> [Flagged Account (fraudFlag=true)]
                                   ↑
                              (1-hop lookup)
                                   
[Transaction] --flagged_by--> [FraudCheckResult]
```

## API Endpoints

### Account Management
- `POST /accounts/{account_id}/flag` - Flag an account as fraudulent
- `DELETE /accounts/{account_id}/flag` - Remove fraud flag from account
- `GET /accounts/flagged` - List all flagged accounts

### Transfer Relationships
- `POST /accounts/{from_account_id}/transfers-to/{to_account_id}` - Create transfer relationship

### Fraud Results
- `GET /fraud-results` - Get paginated fraud detection results
- `GET /transaction/{transaction_id}/fraud-results` - Get fraud results for specific transaction

### Transaction Generation
- `POST /transaction-generation/start?rate=1` - Start generating transactions
- `POST /transaction-generation/stop` - Stop transaction generation

## Testing the System

### Automated Test
Run the provided test script:
```bash
./test_rt1_fraud_detection.sh
```

### Manual Testing

1. **Setup flagged account**:
```bash
curl -X POST "http://localhost:4000/accounts/A00001/flag" \
     -H "Content-Type: application/json" \
     -d '{"reason": "Test fraud account"}'
```

2. **Create transfer relationship**:
```bash
curl -X POST "http://localhost:4000/accounts/A00002/transfers-to/A00001" \
     -H "Content-Type: application/json" \
     -d '{"amount": 1000}'
```

3. **Start transaction generation**:
```bash
curl -X POST "http://localhost:4000/transaction-generation/start?rate=1"
```

4. **Check fraud results**:
```bash
curl "http://localhost:4000/fraud-results" | jq .
```

5. **Stop generation**:
```bash
curl -X POST "http://localhost:4000/transaction-generation/stop"
```

## Expected Results

When a transaction involves account A00002 (which has transferred to flagged account A00001):

```json
{
  "transaction_id": "uuid-here",
  "fraud_score": 90,
  "status": "review",
  "rule": "flaggedAccountsRule",
  "reason": "Connected to 1 flagged account(s)",
  "evaluation_timestamp": "2025-08-02T11:30:00Z"
}
```

## Graph Queries Used

### RT1 Detection Query
```gremlin
// Check sender account for flagged connections
g.V().hasLabel('account').has('account_id', sender_id)
 .out('TRANSFERS_TO').hasLabel('account').has('fraudFlag', true)

// Check receiver account for flagged connections  
g.V().hasLabel('account').has('account_id', receiver_id)
 .out('TRANSFERS_TO').hasLabel('account').has('fraudFlag', true)
```

### Fraud Result Creation
```gremlin
// Create FraudCheckResult vertex
g.addV('FraudCheckResult')
 .property('fraud_score', score)
 .property('status', status)
 .property('rule', 'flaggedAccountsRule')
 .property('evaluation_timestamp', timestamp)
 .property('reason', reason)

// Link to transaction
g.V(transaction).addE('flagged_by').to(fraudResult)
```

## Logging

The system provides comprehensive logging:

- **✅ RT1 CHECK PASSED**: Transaction has no flagged connections
- **🚨 RT1 FRAUD DETECTED**: Transaction flagged with score and reason
- **📊 Created FraudCheckResult**: Fraud result stored in graph
- **🚩 Account flagged**: Account marked as fraudulent
- **💸 Created TRANSFERS_TO edge**: Transfer relationship established

## Configuration

Key parameters in the RT1 system:

- **Fraud Score Range**: 90-100 (based on number of flagged connections)
- **Status Threshold**: ≥95 = "blocked", <95 = "review"  
- **Detection Scope**: 1-hop graph traversal
- **Edge Type**: `TRANSFERS_TO` connections to flagged accounts

## Troubleshooting

### Common Issues

1. **No fraud detected**: 
   - Ensure accounts are properly flagged (`fraudFlag=true`)
   - Verify `TRANSFERS_TO` edges exist between accounts
   - Check that transactions involve connected accounts

2. **Server errors**:
   - Ensure Aerospike Graph database is running (`docker-compose up -d`)
   - Verify graph connection in `/health` endpoint
   - Check server logs for graph client errors

3. **Missing data**:
   - Load users and accounts: `POST /seed-data`
   - Verify accounts exist in user summaries

### Debug Commands

```bash
# Check server health
curl http://localhost:4000/health

# Verify data loaded
curl http://localhost:4000/users?page=1&page_size=5

# Check flagged accounts
curl http://localhost:4000/accounts/flagged

# View recent transactions
curl http://localhost:4000/transactions?page=1&page_size=10
```

## Next Steps

This implementation covers RT1 (flagged accounts). Future fraud detection rules can follow the same pattern:

- **RT2**: Repeated small ring interactions (2-hop neighborhood)
- **RT3**: Supernode detection (high-degree centrality)
- **BT1-BT6**: Batch detection patterns (multi-transaction analysis)

Each rule would create its own `FraudCheckResult` vertices with appropriate rule names and detection logic. 