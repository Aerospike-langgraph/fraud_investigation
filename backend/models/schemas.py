from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class TransactionStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    SUSPICIOUS = "suspicious"
    SAFE = "safe"
    REVIEWED = "reviewed"

class FraudRiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class FraudCheckStatus(str, Enum):
    REVIEW = "review"
    BLOCKED = "blocked"
    CLEARED = "cleared"

class User(BaseModel):
    id: str
    name: str
    email: str
    age: int
    phone: str
    occupation: str
    signup_date: datetime
    location: str
    risk_score: float = Field(ge=0, le=100)
    is_flagged: bool = False

class Account(BaseModel):
    id: str
    user_id: str
    account_type: str
    balance: float
    bank_name: str = ""
    created_date: datetime
    status: str = "active"
    is_active: bool = True
    fraud_flag: bool = False

class Device(BaseModel):
    id: str
    type: str
    os: str
    browser: str
    fingerprint: str
    first_seen: datetime
    last_login: datetime
    login_count: int
    fraud_flag: bool = False

class Transaction(BaseModel):
    id: str
    sender_id: str
    receiver_id: str
    amount: float
    currency: str = "INR"
    timestamp: datetime
    location: str
    device_id: Optional[str] = None
    status: TransactionStatus = TransactionStatus.COMPLETED
    fraud_score: float = Field(ge=0, le=100, default=0.0)
    
    # Enhanced fields for frontend display
    sender_name: Optional[str] = None
    receiver_name: Optional[str] = None
    is_fraud: bool = False
    fraud_rules: List[str] = []
    direction: Optional[str] = None
    original_amount: Optional[float] = None

class FraudCheckResult(BaseModel):
    fraud_score: float = Field(ge=0, le=100)
    status: FraudCheckStatus
    rule: str  # e.g., "flaggedAccountsRule"
    evaluation_timestamp: datetime
    reason: str
    details: Optional[str] = None  # JSON string of additional details

class UserSummary(BaseModel):
    user: User
    accounts: List[Account]
    devices: List[Device]
    recent_transactions: List[Any]  # Allow any type to include enhanced fields
    total_transactions: int
    total_amount_sent: float
    total_amount_received: float
    fraud_risk_level: FraudRiskLevel

class TransactionDetail(BaseModel):
    transaction: Transaction
    sender: User
    receiver: User
    sender_account: Account
    receiver_account: Account
    related_transactions: List[Transaction]
    fraud_indicators: List[str]
    risk_level: FraudRiskLevel

class FraudPattern(BaseModel):
    id: str
    name: str
    description: str
    query: str
    risk_level: FraudRiskLevel

class FraudResult(BaseModel):
    pattern_id: str
    pattern_name: str
    detected_entities: List[Dict[str, Any]]
    risk_score: float
    timestamp: datetime
    details: Dict[str, Any]

class DashboardStats(BaseModel):
    total_users: int
    total_transactions: int
    flagged_transactions: int
    total_amount: float
    fraud_detection_rate: float
    graph_health: str

class SearchResult(BaseModel):
    id: str
    name: str
    type: str
    score: float 