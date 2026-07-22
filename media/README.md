# FinGuard Media Assets

This folder contains demonstration and documentation media for the FinGuard fraud detection project.

## Contents

### 🎬 Streaming Pipeline Demo Video

**File**: `Streaming recording video.mp4` (11 MB)

A live demonstration of the FinGuard real-time fraud detection pipeline in action:

- **Duration**: ~5-7 minutes
- **Shows**:
  - Kafka transaction ingestion in real-time
  - Spark Structured Streaming processing
  - Fraud scoring and anomaly detection
  - Email alert notifications being triggered
  - Dashboard metrics updating in real-time
- **Quality**: 1080p, H.264 codec

**How to Use**: 
Watch this video to see the end-to-end pipeline operating with live transaction data and fraud alerts being processed in milliseconds.

---

### 📊 FinGuard Monitoring Dashboard

**File**: `FinGuard Fraud Detection Monitor 2026-07-13T17-21-15 2026-07-13 18_12.pdf` (448 KB)

A Databricks Lakeview dashboard export showing real-time fraud detection metrics:

- **Metrics Displayed**:
  - High-risk customer counts
  - Fraud alert volume (last 1 hour)
  - Transaction count by minute (sliding window)
  - Alert type distribution (watchlist match, high-value, velocity fraud, etc.)
  - Average transaction amount trends
- **Refresh Interval**: 30 seconds (live update frequency)
- **Data Source**: 
  - `finguard.gold.fraud_card_alert` (primary fraud alerts)
  - `finguard.gold.transaction_count_by_minute_sliding_window` (volume analytics)
  - `finguard.silver.customers` (risk profiling)

**How to Use**:
Open in your PDF viewer or web browser to see a snapshot of the dashboard layout and real-time metrics. This demonstrates the types of queries and visualizations used in production monitoring.

---

## Production Deployment Notes

When deploying FinGuard to production:

1. **Video**: Use for team onboarding and stakeholder demonstrations
2. **Dashboard**: Recreate in Databricks Lakeview using the SQL queries in the main README
3. **Alerts**: Configure email notifications via SMTP/SendGrid (see `02_Setup_Secret_Scope.ipynb`)

---

## File Format Notes

- Video is encoded in H.264 for broad compatibility
- PDF is optimized for screen viewing (not print)
- Both files include timestamps for production audit trails

For issues viewing these files, ensure your system has:
- A modern video player (VLC, QuickTime, Windows Media Player)
- A PDF viewer (Adobe Reader, Chrome, Firefox)
