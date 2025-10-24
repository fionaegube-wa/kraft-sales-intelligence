#  Kraft Sales Data Intelligence Analytics
 Overview

This project demonstrates how Kraft Heinz can enrich internal ERP sales data with external syndicated sources such as Circana and Nielsen, unify the schemas, apply business rules, and compute marketing and sales performance metrics at scale.

The Python script ingests raw files from Google Cloud Storage (GCS) or local folders, standardizes the structure, joins product and customer master data, aligns retail to fiscal calendars, and produces partitioned Parquet outputs ready for ingestion into BigQuery or BI tools (Looker, Tableau, etc.).

Data Flow
Internal ERP Data (GCS) + Circana & Nielsen Drops (GCS) -> Python Enrichment Script -> Standardization + Validation + KPI Computation -> Partitioned Parquet (Curated + Aggregated) -> BigQuery / BI Dashboards

Key Features

- Reads internal ERP and external syndicated data from GCS or local.

- Harmonizes schemas and aligns time periods (retail week → fiscal week).

- Joins product and customer master data.

- Validates and deduplicates records, routing invalid rows to a dead-letter path.
  

Computes key metrics:

- Units & Net Sales

- Average Price per Unit

- Promo / Non-Promo Splits

- Market Share by brand/channel/region

- Outputs partitioned Parquet files with aggregated summaries.
  

Tech Stack

- Python 3.9+

- pandas, pyarrow, fsspec, gcsfs

- Google Cloud Storage (GCS)

- BigQuery (downstream analytics)

How to Run
Local
pip install pandas pyarrow fsspec gcsfs

python kraft_sales_enrichment.py \
  --internal "./sample_data/internal/*.csv" \
  --circana "./sample_data/circana/*.csv" \
  --nielsen "./sample_data/nielsen/*.csv" \
  --product_master "./sample_data/masters/product_master.csv" \
  --customer_master "./sample_data/masters/customer_master.csv" \
  --output_prefix "./output/curated" \
  --badrows_prefix "./output/badrows" \
  --aggregate_dims "retailer,channel,region,brand"

GCS
python kraft_sales_enrichment.py \
  --internal "gs://kraft-data/internal/*.csv" \
  --circana "gs://kraft-data/circana/*.csv" \
  --nielsen "gs://kraft-data/nielsen/*.csv" \
  --product_master "gs://kraft-data/masters/product_master.parquet" \
  --customer_master "gs://kraft-data/masters/customer_master.parquet" \
  --output_prefix "gs://kraft-data/curated" \
  --badrows_prefix "gs://kraft-data/badrows" \
  --aggregate_dims "retailer,channel,region,brand"
