# 🏎️ PySpark Data Quality for Racing Data 🏎️

[![Python 3.9.11](https://img.shields.io/badge/Python-3.9.11-blue.svg?logo=python&logoColor=white)](https://www.python.org/downloads/release/python-3911/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)](https://spark.apache.org/)
[![Status](https://img.shields.io/badge/Report-HTML-green.svg)](#outputs)

A small, focused PySpark job that validates a racing telemetry CSV, fixes column names, runs a handful of integrity checks, and generates a clean HTML report + per-check CSVs of violations.

---

## What it does

- Cleans header names (strips whitespace, converts spaces → underscores)
- Runs data-quality checks:
  - **Required columns present**: `event_id, session, lap, timestamp_ms, driver, team, car_no, tyre`
  - **Primary-key uniqueness** on `event_id, session, car_no, lap`
  - **Timestamp format**: `timestamp_ms` must be **13 digits**
  - **Monotonicity per car**: laps don’t go backwards; timestamps don’t decrease within `(event_id, session, car_no)`
  - **Reasonable ranges**  
    - `air_temp_c ∈ [0, 30]`  
    - `track_temp_c ∈ [-20, 80]`  
    - `battery_soc ∈ [0.6, 40]`  
    - `fuel_kg ≥ 0`
  - **Conditional rule**: if `tyre == "Wet"`, then `sensor_0008_val ∈ [100, 250]`
  - **Domain check**: `tyre ∈ {Soft, Medium, Hard, Wet, Intermediate}`

---

## Requirements

- Python **3.8+**
- Java **8 or 11**
- Apache Spark **3.x**
- PySpark

```bash
pip install pyspark
# verify
python -c "import pyspark; print(pyspark.__version__)"
