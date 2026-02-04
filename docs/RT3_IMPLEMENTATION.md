# RT3 Fraud Detection Implementation
## Supernode Detection (High-Degree Account Detection)

### Overview
RT3 fraud detection identifies "supernodes" - accounts that receive transactions from an unusually high number of unique sender accounts within a specified time window. This pattern often indicates money laundering schemes, mule accounts, or other fraudulent activities.

### Implementation Details

#### Configuration (`backend/config/rt3_config.py`)
- **Lookback Period**: 30 days (configurable)
- **Threshold**: 50 unique senders (configurable)
- **High-Risk Threshold**: 100 unique senders
- **Scoring Algorithm**: Base score of 40 + 0.5 points per sender above threshold
- **Status Thresholds**: 
  - Review: Score ≥ 60
  - Block: Score ≥ 85

#### Detection Logic (`backend/services/transaction_generator.py`)
The RT3 detection runs automatically after each transaction is generated:

1. **Trigger**: Every new transaction triggers RT3 analysis on the receiver account
2. **Analysis**: Counts unique sender accounts that have sent transactions to the receiver in the last N days
3. **Scoring**: Calculates fraud score based on the number of unique senders
4. **Action**: Creates fraud check results for transactions that exceed the threshold

#### Gremlin Query
```gremlin
g.V().has_label("account").has("account_id", "{receiver_account_id}")
.in_("TRANSFERS_TO")
.has("timestamp", P.gte("{lookback_timestamp}"))
.in_("TRANSFERS_TO")
.dedup()
.count()
```

### Integration Points

#### Real-time Detection
- Integrated into the transaction generation loop
- Runs after RT1 (flagged account detection)
- Creates `FraudCheckResult` vertices in the graph database
- Links fraud results to transactions via `flagged_by` edges

#### Fraud Check Results
Each detected supernode creates:
- **Fraud Score**: Calculated based on unique sender count
- **Status**: "review" or "blocked" based on score
- **Rule**: "RT3_SupernodeRule"
- **Details**: Includes sender count, threshold, and sample sender data

### Configuration Options

All RT3 settings are configurable in `backend/config/rt3_config.py`:

```python
class RT3Config:
    LOOKBACK_DAYS = 30                    # Time window for analysis
    MIN_UNIQUE_SENDERS_THRESHOLD = 50     # Minimum senders to trigger alert
    HIGH_RISK_THRESHOLD = 100             # High-risk sender count
    BASE_FRAUD_SCORE = 40                 # Base score when threshold exceeded
    SENDER_MULTIPLIER = 0.5               # Score increment per excess sender
    REVIEW_THRESHOLD = 60                 # Score for "review" status
    BLOCK_THRESHOLD = 85                  # Score for "blocked" status
```

### Logging and Monitoring

RT3 detection provides comprehensive logging:
- ✅ **Pass**: Normal connection patterns
- 🚨 **Fraud Detected**: Supernode patterns identified
- 📊 **Analysis Results**: Detailed sender count and scoring
- ❌ **Errors**: Graph query or processing issues

### Example Detection Flow

1. **Transaction Created**: User A sends $100 to Account X
2. **RT3 Triggered**: Analyze Account X (receiver)
3. **Graph Query**: Count unique senders to Account X in last 30 days
4. **Result**: Account X received from 75 unique senders
5. **Scoring**: Score = 40 + (75-50) × 0.5 = 52.5
6. **Action**: No fraud (below review threshold of 60)

### Performance Considerations

- **Query Optimization**: Uses deduplication and count operations for efficiency
- **Caching**: 10-minute cache expiry to avoid repeated calculations
- **Limit Clauses**: Sample data limited to 100 senders for detailed reporting
- **Asynchronous Execution**: Non-blocking fraud detection

### Data Usage

RT3 uses **only real transaction data** from the graph database:
- No mock data or hardcoded values
- Real timestamp filtering
- Actual account relationships
- Live transaction history

This implementation provides robust supernode detection for real-time fraud prevention while maintaining high performance and configurability. 