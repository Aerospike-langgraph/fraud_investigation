"""
ML Model Placeholder Service

This is a placeholder service that simulates ML model predictions for fraud risk scoring.
Replace this with actual ML model integration when ready.
"""

import logging
import random
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger('fraud_detection.ml')


class MLModelService:
    """
    Placeholder ML Model Service for fraud risk prediction.
    
    This service simulates risk scoring based on account features.
    Replace the predict_risk method with actual ML model calls when integrating
    with a real ML service.
    """
    
    def __init__(self):
        self.model_version = "placeholder-v1.0"
        self.last_prediction_time = None
        
    def predict_risk(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict fraud risk score for an account based on its features.
        
        Args:
            features: Dictionary containing account features:
                - transaction_count: Number of transactions
                - total_amount: Total transaction amount
                - avg_amount: Average transaction amount
                - device_count: Number of devices used
                - unique_recipients: Number of unique transaction recipients
                - flagged_connections: Number of connections to flagged accounts
                - account_age_days: Age of the account in days
                - high_value_txn_count: Number of high-value transactions
                
        Returns:
            Dictionary containing:
                - risk_score: Float 0-100
                - risk_factors: List of contributing factors
                - confidence: Model confidence score
        """
        self.last_prediction_time = datetime.now()
        
        # Extract features with defaults
        txn_count = features.get('transaction_count', 0)
        total_amount = features.get('total_amount', 0)
        avg_amount = features.get('avg_amount', 0)
        device_count = features.get('device_count', 1)
        unique_recipients = features.get('unique_recipients', 0)
        flagged_connections = features.get('flagged_connections', 0)
        account_age_days = features.get('account_age_days', 365)
        high_value_txn_count = features.get('high_value_txn_count', 0)
        
        # Calculate risk score based on various factors (placeholder logic)
        risk_score = 0.0
        risk_factors = []
        
        # Factor 1: High transaction velocity
        if txn_count > 50:
            velocity_score = min(20, (txn_count - 50) * 0.5)
            risk_score += velocity_score
            if velocity_score > 10:
                risk_factors.append(f"High transaction velocity ({txn_count} transactions)")
        
        # Factor 2: High average transaction amount
        if avg_amount > 5000:
            amount_score = min(20, (avg_amount - 5000) / 500)
            risk_score += amount_score
            if amount_score > 10:
                risk_factors.append(f"High average transaction amount (${avg_amount:,.2f})")
        
        # Factor 3: Multiple devices
        if device_count > 3:
            device_score = min(15, (device_count - 3) * 3)
            risk_score += device_score
            if device_score > 5:
                risk_factors.append(f"Multiple devices used ({device_count} devices)")
        
        # Factor 4: Connections to flagged accounts (high weight)
        if flagged_connections > 0:
            flagged_score = min(30, flagged_connections * 10)
            risk_score += flagged_score
            risk_factors.append(f"Connected to {flagged_connections} flagged account(s)")
        
        # Factor 5: New account with high activity
        if account_age_days < 30 and txn_count > 20:
            new_account_score = min(15, 15 - (account_age_days / 2))
            risk_score += new_account_score
            risk_factors.append(f"New account with high activity ({account_age_days} days old)")
        
        # Factor 6: High value transactions
        if high_value_txn_count > 5:
            high_value_score = min(15, high_value_txn_count * 2)
            risk_score += high_value_score
            if high_value_score > 5:
                risk_factors.append(f"Multiple high-value transactions ({high_value_txn_count})")
        
        # Factor 7: Many unique recipients (potential money laundering pattern)
        if unique_recipients > 30:
            recipients_score = min(15, (unique_recipients - 30) * 0.5)
            risk_score += recipients_score
            if recipients_score > 5:
                risk_factors.append(f"Many unique recipients ({unique_recipients})")
        
        # Add some randomness to simulate ML model variance (±5%)
        variance = random.uniform(-5, 5)
        risk_score = max(0, min(100, risk_score + variance))
        
        # Calculate confidence based on feature completeness
        feature_count = sum(1 for v in features.values() if v is not None and v != 0)
        confidence = min(0.95, 0.5 + (feature_count * 0.05))
        
        # Generate reason string
        if risk_factors:
            reason = " | ".join(risk_factors[:3])  # Top 3 factors
        else:
            reason = "Normal account activity"
        
        return {
            "risk_score": round(risk_score, 2),
            "risk_factors": risk_factors,
            "reason": reason,
            "confidence": round(confidence, 2),
            "model_version": self.model_version,
            "prediction_time": self.last_prediction_time.isoformat()
        }
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the ML model."""
        return {
            "model_version": self.model_version,
            "model_type": "placeholder",
            "description": "Placeholder ML model for fraud risk scoring. Replace with actual ML integration.",
            "features_used": [
                "transaction_count",
                "total_amount", 
                "avg_amount",
                "device_count",
                "unique_recipients",
                "flagged_connections",
                "account_age_days",
                "high_value_txn_count"
            ],
            "last_prediction_time": self.last_prediction_time.isoformat() if self.last_prediction_time else None
        }


# Singleton instance
ml_model_service = MLModelService()
