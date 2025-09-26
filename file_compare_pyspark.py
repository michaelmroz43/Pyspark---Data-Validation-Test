from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime
import os, re, html

csv_path = os.path.join(os.getcwd(), "racing_sample_1000rows_500sensors.csv")
sep = ","  # change to '|' if needed
out_dir = os.path.join(os.getcwd(), "dq_report")
os.makedirs(out_dir, exist_ok=True)

def clean_cols(df):
    names = []
    for c in df.columns:
        n = c.replace("\ufeff", "").strip()
        n = re.sub(r"\s+", "_", n)
        names.append(n)
    return df.toDF(*names)

def sample_rows(df, cols, n=20):
    cols = [c for c in cols if c in df.columns]
    if not cols or df is None:
        return []
    return [r.asDict() for r in df.select(*cols).limit(n).collect()]

def html_table(rows, cols):
    if not rows:
        return "<em>No sample rows</em>"
    if not cols:
        cols = list({k for r in rows for k in r.keys()})
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = []
    for r in rows:
        def _safe_str(v):
            return "" if v is None else str(v)

        tds = "".join(
            f"<td>{html.escape(_safe_str(r.get(c)))}</td>"
            for c in cols
        )
        body.append(f"<tr>{tds}</tr>")
    return f"<table class='mini'><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"

def write_violations(df, name):
    if df is None:
        return None
    df_count = df.count()
    if df_count == 0:
        return None
    dest = os.path.join(out_dir, "violations", name)
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(dest))
    return dest

def add_result(results, name, df_count, sample_cols=None, note=""):
    if hasattr(df_count, "count"):
        count = df_count.count()
        samp_rows = sample_rows(df_count, sample_cols or [])
        csv_path = write_violations(df_count, name)
    else:
        count = int(df_count)
        samp_rows, csv_path = [], None
    results.append({
        "name": name,
        "violations": count,
        "status": "PASS" if count == 0 else "FAIL",
        "sample_cols": sample_cols or [],
        "sample": samp_rows,
        "csv": csv_path,
        "note": note
    })

# ---- spark / load ----
spark = (SparkSession.builder
         .appName("racing_dq")
         .master("local[*]")
         .getOrCreate())

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .option("sep", sep)
      .option("quote", '"').option("escape", '"')
      .csv(csv_path))

df = clean_cols(df)
total_rows = df.count()

# ---- checks ----
results = []

# required columns
required = ["event_id","session","lap","timestamp_ms","driver","team","car_no","tyre"]
missing = [c for c in required if c not in df.columns]
add_result(results, "required_columns_present", 0 if not missing else 1,
           note=("All present" if not missing else f"Missing: {', '.join(missing)}"))

# pk uniqueness
pk = ["event_id","session","car_no","lap"]
if all(c in df.columns for c in pk):
    dupes = df.groupBy(*pk).count().filter(F.col("count") > 1)
    add_result(results, "pk_unique", dupes, sample_cols=pk+["count"])
else:
    add_result(results, "pk_unique", 1, note=f"Missing PK cols: {set(pk) - set(df.columns)}")

# timestamp format (13 digits)
bad_ts = df.filter(F.col("timestamp_ms").isNull() |
                   ~F.col("timestamp_ms").cast("string").rlike(r"^\d{13}$"))
add_result(results, "timestamp_ms_is_13_digits", bad_ts,
           sample_cols=["event_id","lap","timestamp_ms"])

# for each car, does the timestamp go backward as the laps increase
need = ["event_id","session","car_no","lap","timestamp_ms"]
if all(c in df.columns for c in need):
    w = Window.partitionBy("event_id","session","car_no").orderBy(F.col("lap").cast("long"))
    d2 = (df.withColumn("lap_long", F.col("lap").cast("long"))
            .withColumn("ts_long",  F.col("timestamp_ms").cast("long"))
            .withColumn("prev_lap", F.lag("lap_long").over(w))
            .withColumn("prev_ts",  F.lag("ts_long").over(w)))
    lap_back = d2.filter(F.col("prev_lap").isNotNull() & (F.col("lap_long") < F.col("prev_lap")))
    ts_back  = d2.filter(F.col("prev_ts").isNotNull()  & (F.col("ts_long")  < F.col("prev_ts")))
    add_result(results, "lap_not_decreasing_per_car", lap_back,
               sample_cols=["event_id","session","car_no","prev_lap","lap"])
    add_result(results, "timestamp_not_decreasing_per_car", ts_back,
               sample_cols=["event_id","session","car_no","prev_ts","timestamp_ms"])
else:
    add_result(results, "lap_not_decreasing_per_car", 1, note="missing columns")
    add_result(results, "timestamp_not_decreasing_per_car", 1, note="missing columns")

# verify data ranges
bad_air   = df.filter( F.col("air_temp_c").cast("double").isNull() |
                      (F.col("air_temp_c").cast("double") < 0) |
                      (F.col("air_temp_c").cast("double") > 30) )
bad_track = df.filter( F.col("track_temp_c").cast("double").isNull() |
                      (F.col("track_temp_c").cast("double") < -20) |
                      (F.col("track_temp_c").cast("double") > 80) )
bad_soc   = df.filter( F.col("battery_soc").cast("double").isNull() |
                      (F.col("battery_soc").cast("double") < 0.6) |
                      (F.col("battery_soc").cast("double") > 40) )
bad_fuel  = df.filter( F.col("fuel_kg").cast("double").isNull() |
                      (F.col("fuel_kg").cast("double") < 0) )

add_result(results, "air_temp_c_in_0_30", bad_air,   sample_cols=["event_id","lap","air_temp_c"])
add_result(results, "track_temp_c_in_-20_80", bad_track, sample_cols=["event_id","lap","track_temp_c"])
add_result(results, "battery_soc_in_0.6_40", bad_soc, sample_cols=["event_id","lap","battery_soc"])
add_result(results, "fuel_kg_non_negative", bad_fuel, sample_cols=["event_id","lap","fuel_kg"])

# conditional example: wet -> sensor_0008_val in [100,250]
if "tyre" in df.columns and "sensor_0008_val" in df.columns:
    is_wet = F.lower(F.trim(F.col("tyre"))) == F.lit("wet")
    v8 = F.col("sensor_0008_val").cast("double")
    bad_wet = df.filter(is_wet & (v8.isNull() | ~v8.between(100.0, 250.0)))
    add_result(results, "if_wet_then_sensor_0008_val_100_250",
               bad_wet, sample_cols=["event_id","lap","tyre","sensor_0008_val"])
else:
    add_result(results, "if_wet_then_sensor_0008_val_100_250", 1, note="missing tyre or sensor_0008_val")

# tyre domain
if "tyre" in df.columns:
    allowed = ["Soft","Medium","Hard","Wet","Intermediate"]
    bad_tyre = df.filter(~F.lower("tyre").isin([t.lower() for t in allowed]))
    add_result(results, "tyre_in_domain", bad_tyre, sample_cols=["event_id","lap","tyre"],
               note=f"Allowed: {allowed}")
else:
    add_result(results, "tyre_in_domain", 1, note="missing tyre")

# ---- HTML ----
summary_rows = []
for r in results:
    link = f"<a href='{r['csv']}' target='_blank'>CSV</a>" if r['csv'] else ""
    sample = html_table(r["sample"], r["sample_cols"]) if r["sample"] else "<em>No sample rows</em>"
    note = f"<div class='note'>{html.escape(r['note'])}</div>" if r["note"] else ""
    summary_rows.append(f"""
      <tr class="{r['status'].lower()}">
        <td class="name">{html.escape(r['name'])}</td>
        <td><span class="badge {r['status'].lower()}'>{r['status']}</span></td>
        <td class="cnt">{r['violations']}</td>
        <td>{link}</td>
      </tr>
      <tr class="sample"><td colspan="4">{note}<details><summary>Sample</summary>{sample}</details></td></tr>
    """)

html_out = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Racing DQ Report</title>
<style>
 body {{ font-family: Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }}
 h1 {{ margin: 0 0 6px; }}
 .meta {{ color:#666; margin-bottom:16px; }}
 table.summary {{ border-collapse: collapse; width: 100%; }}
 table.summary th, table.summary td {{ border-bottom:1px solid #eee; padding:8px; text-align:left; }}
 .name {{ font-weight:600; }}
 .cnt {{ text-align:right; width:120px; }}
 .badge {{ padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid #ddd; }}
 .badge.pass {{ background:#eaf7ee; color:#106a3a; border-color:#cfead8; }}
 .badge.fail {{ background:#fdeceb; color:#b00020; border-color:#f6b3ad; }}
 tr.sample td {{ background:#fafafa; }}
 table.mini {{ border-collapse: collapse; width:100%; font-size:12px; margin-top:8px; }}
 table.mini th, table.mini td {{ border:1px solid #e6e6e6; padding:4px 6px; }}
 .note {{ color:#444; margin-bottom:6px; }}
</style>
</head>
<body>
  <h1>Racing Data Quality</h1>
  <div class="meta">
    File: {html.escape(os.path.basename(csv_path))} &nbsp;|&nbsp;
    Rows: {total_rows} &nbsp;|&nbsp;
    Cols: {len(df.columns)} &nbsp;|&nbsp;
    Delimiter: {html.escape(sep)} &nbsp;|&nbsp;
    Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  </div>

  <table class="summary">
    <thead>
      <tr><th>Test</th><th>Status</th><th>Violations</th><th>Export</th></tr>
    </thead>
    <tbody>
      {''.join(summary_rows)}
    </tbody>
  </table>
</body>
</html>
"""

with open(os.path.join(out_dir, "dq_report.html"), "w", encoding="utf-8") as f:
    f.write(html_out)

print(f"Report: {os.path.join(out_dir, 'dq_report.html')}")
print(f"Violations (if any): {os.path.join(out_dir, 'violations')}")
