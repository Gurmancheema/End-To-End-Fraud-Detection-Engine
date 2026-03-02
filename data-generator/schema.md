# Transaction Event Schema

This document defines the data contract for the real-time fraud detection platform.

Each event represents a single financial transaction generated in real-time and published to Kafka.

---

## 1. Identity Fields

### transaction_id (string - UUID)
Unique identifier for each transaction.
Used for traceability and idempotency.

### user_id (string)
Unique identifier for the customer performing the transaction.
Used for behavioral aggregation and fraud pattern detection.

### merchant_id (string)
Identifier for the merchant receiving the payment.
Used to detect merchant-specific fraud trends.

---

## 2. Monetary Fields

### transaction_amount (double)
Amount of money involved in the transaction.
Critical signal for anomaly detection.

### currency (string)
Currency code (e.g., INR, USD).
Useful in multi-region fraud scenarios.

### transaction_type (string)
Type of transaction:
- debit
- credit
- withdrawal
- online_payment
- POS_payment

Used to differentiate fraud behavior patterns.

---

## 3. Behavioral Fields

### device_id (string)
Identifier of device used for the transaction.
Used to detect device-switch fraud.

### location (string)
City or region where transaction occurred.
Used for geolocation anomaly detection.

### payment_method (string)
Method used:
- card
- UPI
- net_banking
- wallet

Different fraud risks exist for different payment methods.

---

## 4. Temporal Field

### transaction_timestamp (timestamp)
Time at which transaction occurred.
Required for velocity-based fraud detection and windowing.

---

## 5. User Profile Signals

### user_account_age_days (integer)
Number of days since account creation.
New accounts carry higher fraud risk.

### user_avg_transaction_amount (double)
Historical average transaction amount of user.
Used for deviation-based anomaly detection.

---

## 6. Label (For Training Only)

### is_fraud (integer: 0 or 1)
Indicates whether transaction is fraudulent.
Used for supervised ML training.
In real production streaming systems, this field is not available at inference time.
