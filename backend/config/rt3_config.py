"""
RT3 Fraud Detection Configuration
Supernode Detection (High-Degree Account Detection)

This module contains configuration settings for the RT3 fraud detection method
which identifies accounts that receive transactions from an unusually high 
number of different sender accounts within a specified time window.
"""

from datetime import timedelta

class RT3Config:
    """Configuration for RT3 Supernode Detection"""
    
    # Time window for checking transaction history (in days)
    LOOKBACK_DAYS = 30
    
    # Minimum number of unique sender accounts to trigger fraud alert
    MIN_UNIQUE_SENDERS_THRESHOLD = 50
    
    # High-risk threshold for unique senders (immediate block)
    HIGH_RISK_THRESHOLD = 100
    
    # Fraud score calculation weights
    BASE_FRAUD_SCORE = 40  # Base score when threshold is exceeded
    SENDER_MULTIPLIER = 0.5  # Additional score per unique sender above threshold
    MAX_FRAUD_SCORE = 100
    
    # Fraud status thresholds
    REVIEW_THRESHOLD = 60    # Score >= 60: mark for review
    BLOCK_THRESHOLD = 85     # Score >= 85: block transaction
    
    # Caching settings (to avoid repeated calculations)
    CACHE_EXPIRY_MINUTES = 10
    
    @classmethod
    def get_lookback_timestamp(cls):
        """Get the timestamp for lookback period"""
        from datetime import datetime
        return datetime.now() - timedelta(days=cls.LOOKBACK_DAYS)
    
    @classmethod
    def calculate_fraud_score(cls, unique_sender_count: int) -> float:
        """Calculate fraud score based on unique sender count"""
        if unique_sender_count < cls.MIN_UNIQUE_SENDERS_THRESHOLD:
            return 0.0
        
        # Base score + additional score for each sender above threshold
        excess_senders = unique_sender_count - cls.MIN_UNIQUE_SENDERS_THRESHOLD
        score = cls.BASE_FRAUD_SCORE + (excess_senders * cls.SENDER_MULTIPLIER)
        
        return min(score, cls.MAX_FRAUD_SCORE)
    
    @classmethod
    def get_fraud_status(cls, fraud_score: float) -> str:
        """Determine fraud status based on score"""
        if fraud_score >= cls.BLOCK_THRESHOLD:
            return "blocked"
        elif fraud_score >= cls.REVIEW_THRESHOLD:
            return "review"
        else:
            return "clean"
    
    @classmethod
    def get_fraud_reason(cls, unique_sender_count: int) -> str:
        """Generate fraud reason message"""
        return f"Supernode detected: Account received transactions from {unique_sender_count} unique senders in the last {cls.LOOKBACK_DAYS} days (threshold: {cls.MIN_UNIQUE_SENDERS_THRESHOLD})"

# Export the configuration for easy import
RT3_CONFIG = RT3Config() 