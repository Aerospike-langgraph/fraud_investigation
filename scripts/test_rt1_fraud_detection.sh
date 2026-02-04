#!/bin/bash

# RT1 Fraud Detection Test Script
# This script demonstrates the RT1 fraud detection system

BASE_URL="http://localhost:4000"

echo "🧪 RT1 Fraud Detection Test Script"
echo "======================================"

# Check if server is running
echo "📡 Checking if server is running..."
if ! curl -s "$BASE_URL/health" > /dev/null; then
    echo "❌ Server is not running. Please start the backend server first."
    exit 1
fi
echo "✅ Server is running"

# Get some account IDs from users
echo ""
echo "👥 Getting account IDs for testing..."
ACCOUNTS=$(curl -s "$BASE_URL/users?page=1&page_size=5" | jq -r '.users[].id' | head -3)
ACCOUNT_ARRAY=($ACCOUNTS)

if [ ${#ACCOUNT_ARRAY[@]} -lt 3 ]; then
    echo "❌ Not enough users/accounts found. Please ensure data is loaded."
    exit 1
fi

# Get actual account IDs by looking up user summaries
echo "🔍 Looking up account IDs..."
ACCOUNT1=$(curl -s "$BASE_URL/user/${ACCOUNT_ARRAY[0]}/summary" | jq -r '.accounts[0].id // empty')
ACCOUNT2=$(curl -s "$BASE_URL/user/${ACCOUNT_ARRAY[1]}/summary" | jq -r '.accounts[0].id // empty')
ACCOUNT3=$(curl -s "$BASE_URL/user/${ACCOUNT_ARRAY[2]}/summary" | jq -r '.accounts[0].id // empty')

if [ -z "$ACCOUNT1" ] || [ -z "$ACCOUNT2" ] || [ -z "$ACCOUNT3" ]; then
    echo "❌ Could not find valid accounts. Please check data setup."
    exit 1
fi

echo "✅ Found accounts: $ACCOUNT1, $ACCOUNT2, $ACCOUNT3"

# Step 1: Flag Account3 as fraudulent
echo ""
echo "🚩 Step 1: Flagging account $ACCOUNT3 as fraudulent..."
curl -s -X POST "$BASE_URL/accounts/$ACCOUNT3/flag" \
     -H "Content-Type: application/json" \
     -d '{"reason": "Test account flagged for RT1 demonstration"}' | jq .

# Step 2: Create a TRANSFERS_TO relationship from Account2 to flagged Account3
echo ""
echo "💸 Step 2: Creating transfer relationship $ACCOUNT2 → $ACCOUNT3..."
curl -s -X POST "$BASE_URL/accounts/$ACCOUNT2/transfers-to/$ACCOUNT3" \
     -H "Content-Type: application/json" \
     -d '{"amount": 5000}' | jq .

# Step 3: Check flagged accounts
echo ""
echo "📋 Step 3: Checking flagged accounts..."
curl -s "$BASE_URL/accounts/flagged" | jq .

# Step 4: Start transaction generation to trigger RT1 detection
echo ""
echo "🔄 Step 4: Starting transaction generation..."
curl -s -X POST "$BASE_URL/transaction-generation/start?rate=1" | jq .

echo ""
echo "⏳ Waiting 10 seconds for transactions to be generated..."
sleep 10

# Step 5: Check for fraud detection results
echo ""
echo "🕵️ Step 5: Checking fraud detection results..."
curl -s "$BASE_URL/fraud-results" | jq .

# Step 6: Get recent transactions to see which ones were flagged
echo ""
echo "📊 Step 6: Getting recent transactions..."
curl -s "$BASE_URL/transactions?page=1&page_size=5" | jq '.transactions[] | {id, sender_id, receiver_id, amount, timestamp}'

# Step 7: Stop transaction generation
echo ""
echo "🛑 Step 7: Stopping transaction generation..."
curl -s -X POST "$BASE_URL/transaction-generation/stop" | jq .

# Step 8: Final fraud results summary
echo ""
echo "📈 Step 8: Final fraud detection summary..."
FRAUD_COUNT=$(curl -s "$BASE_URL/fraud-results" | jq '.fraud_results | length')
echo "Total fraud detections: $FRAUD_COUNT"

if [ "$FRAUD_COUNT" -gt 0 ]; then
    echo "🎉 RT1 fraud detection system is working!"
    echo ""
    echo "📋 Fraud Results Summary:"
    curl -s "$BASE_URL/fraud-results" | jq '.fraud_results[] | {transaction_id, fraud_score, status, rule, reason}'
else
    echo "⚠️ No fraud detected. This could mean:"
    echo "   - No transactions involved the flagged account connections"
    echo "   - RT1 detection is not working properly"
    echo "   - Check server logs for errors"
fi

echo ""
echo "🧹 Cleanup: Unflagging test account..."
curl -s -X DELETE "$BASE_URL/accounts/$ACCOUNT3/flag" | jq .

echo ""
echo "✅ RT1 Fraud Detection Test Complete!"
echo ""
echo "📖 What happened:"
echo "1. Account $ACCOUNT3 was flagged as fraudulent"
echo "2. A transfer relationship was created: $ACCOUNT2 → $ACCOUNT3"
echo "3. Transactions were generated"
echo "4. RT1 rule checked if any transaction senders/receivers had connections to flagged accounts"
echo "5. If $ACCOUNT2 was involved in a transaction, it should have been flagged by RT1"
echo ""
echo "🔍 Check the fraud results above to see if RT1 detected any suspicious transactions!" 