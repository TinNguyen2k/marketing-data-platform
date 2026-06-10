# Marketing Data Platform

## Overview

End-to-end marketing data pipeline designed to centralize advertising data from multiple platforms into BigQuery for analytics and reporting.

## Architecture

Google Ads / Facebook Ads / TikTok Ads / Apple Search Ads

↓

Python API Connectors

↓

Apache Airflow

↓

BigQuery

↓

Looker Studio

## Tech Stack

- Python
- SQL
- BigQuery
- Apache Airflow
- Looker Studio
- REST APIs

## Features

### Data Ingestion
- Extract data from multiple advertising APIs.
- Support incremental loading.
- Handle authentication and pagination.

### Workflow Orchestration
- Schedule and automate ETL pipelines using Airflow.
- Monitor pipeline execution and data freshness.

### Analytics & Reporting
- Analyze campaign metrics including Cost, Impression, Click, CTR, CPC, CPM, and Conversion.
- Build dashboards for campaign performance monitoring.

## Business Impact

- Centralized marketing data from multiple advertising platforms.
- Reduced manual reporting efforts through automated ETL workflows.
- Enabled faster and more reliable campaign performance analysis.
