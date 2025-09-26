# Pyspark---Data-Validation-Test
Loads racing CSV into Spark, cleans column names, and runs data checks: required cols, PK uniqueness, 13-digit timestamps, laps/time non-decreasing per car, value ranges, tyre domain, and wet-tyre rule (sensor_0008_val 100–250). Outputs HTML report and CSV violations.
