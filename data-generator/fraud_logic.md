# Fraud Logic Design

This document defines simulated fraud behavior patterns used to generate realistic fraudulent transactions.

Fraud is not random. It follows behavioral anomalies and risk triggers.

We simulate multiple fraud scenarios to reflect real-world financial fraud.

---

## Fraud Scenario 1: High Amount Anomaly

Condition:
- transaction_amount > 3 × user_avg_transaction_amount

Reason:
Fraudulent transactions often involve unusually large amounts compared to user's normal spending behavior.

Trigger:
Mark transaction as potentially fraudulent if threshold exceeded.

---

## Fraud Scenario 2: Velocity Fraud

Condition:
- More than 5 transactions by same user within 60 seconds

Reason:
Fraudsters often execute rapid repeated transactions before account is blocked.

Trigger:
If rapid transactions detected, increase fraud probability.

---

## Fraud Scenario 3: Location Anomaly

Condition:
- Current transaction location differs significantly from previous location
- Time difference between transactions < 5 minutes

Reason:
Physical travel between distant locations in short time is impossible.
This indicates account compromise.

Trigger:
Flag suspicious geolocation change.

---

## Fraud Scenario 4: New Device High-Risk Usage

Condition:
- device_id not seen before for this user
- transaction_amount is high

Reason:
Fraudsters often use new devices to bypass device fingerprinting systems.

Trigger:
Increase fraud likelihood if both conditions met.

---

## Fraud Scenario 5: Young Account Large Transaction

Condition:
- user_account_age_days < 7
- transaction_amount above threshold

Reason:
Fraud accounts are often newly created and used for high-value transactions quickly.

Trigger:
Mark as suspicious.

---

## Fraud Decision Strategy

Fraud probability increases with number of triggered conditions.

Example:
- 1 condition triggered → 30% fraud probability
- 2 conditions → 60%
- 3+ conditions → 90%

Final is_fraud label is assigned based on cumulative risk.

This approach ensures fraud distribution is realistic rather than purely random.
