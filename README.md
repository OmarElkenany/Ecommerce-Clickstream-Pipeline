# E-commerce Clickstream Data Engineering Pipeline

An end-to-end Big Data Engineering project built using Microsoft Fabric
to process e-commerce clickstream events and transform raw customer
interactions into business-ready analytical data.

## Project Overview

This project implements an end-to-end data engineering pipeline
for analyzing customer behavior throughout the e-commerce journey,
from product views and cart interactions to checkout and purchases.

The platform processes historical and continuously generated event
data using a Medallion Architecture, transforming raw data into
structured analytical models and business insights.

## Architecture

The pipeline follows the Medallion Architecture:

- **Data Generation:** Python-based event generator.
- **Ingestion:** Kafka-compatible streaming endpoint and Microsoft
  Fabric Eventstream.
- **Event Processing:** Eventhouse receives events in `raw_events`
  and distributes them into raw tables through update policies.
- **Bronze Layer:** Stores ingested source data.
- **Silver Layer:** Applies data cleaning, schema validation,
  deduplication, and valid/invalid record separation using Spark.
- **Gold Layer:** Builds analytical dimensions, fact tables,
  and business-oriented aggregate tables.
- **Data Quality:** Dedicated validation notebooks for Silver
  and Gold outputs.
- **Orchestration:** Microsoft Fabric Data Pipeline.
- **Visualization:** Power BI dashboard.

## Data Modeling

The Gold Layer uses a Galaxy Schema (Fact Constellation)
to support analytical queries across customer behavior,
products, sessions, and orders.

It contains:

- 5 Dimension Tables
- 3 Fact Tables
- 7 Analytical Tables

### Slowly Changing Dimensions

The `dim_product` dimension implements Slowly Changing
Dimension Type 2 (SCD Type 2).

Product attribute changes create new historical versions,
using effective timestamps and an `is_current` flag.

Fact tables use temporal joins to associate events and
orders with the product version valid at the corresponding
event or order timestamp.

## Data Quality

Dedicated Data Quality notebooks validate the Silver
and Gold layers.

Project Data Quality results:

- Score: 91.8%
- PASS: 112
- WARN: 10
- FAIL: 0
- INFO: 59

The score represents the outcome of applicable quality checks,
not the percentage of valid records.

## Power BI Dashboard

The dashboard provides insights into:

- Customer conversion.
- Cart abandonment.
- Product and category activity.
- Device usage.
- Geographic cart activity.
- Revenue and customer behavior.

## Project Deliverables

- `src/`: Python source scripts.
- `powerbi/`: Dashboard PDF export.
- `presentation/`: Project presentation.

## Technologies

- Python
- Apache Spark / PySpark
- Apache Kafka-compatible ingestion
- Microsoft Fabric
- Eventstream
- Eventhouse
- Delta Lake
- Power BI

## Repository Structure

```text
.
├── src/
│   ├── analysis/
│   │   └── EDA.py
│   ├── pipeline/
│   │   ├── Silver_Layer.py
│   │   └── Gold.py
│   └── data_quality/
│       ├── Silver_DataQuality.py
│       └── Gold_DataQuality.py
├── powerbi/
│   └── Dashboard.pdf
├── presentation/
│   └── Ecommerce Clickstream.pptx
├── README.md
└── .gitignore
```

## Execution

The Python scripts are designed to run within the configured
Microsoft Fabric environment.

Execution requires the corresponding Fabric Lakehouse,
source tables, and pipeline configuration.

## Acknowledgment

Developed as part of the Samsung Innovation Campus
Big Data program.
