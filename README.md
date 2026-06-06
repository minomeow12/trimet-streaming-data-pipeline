# TriMet BreadCrumb Data Pipeline

> **Portland State University — Data Engineering, Spring 2026**  
> 📽️ [Watch our project presentation](https://youtu.be/B06P8KBr5HE?si=H46JdPVKJSuGnL9V)

A fully automated, cloud-based data pipeline that collects, transports, validates, transforms, stores, and visualizes real-time GPS and stop-event data from TriMet's Portland bus fleet — built on Google Cloud Platform using Pub/Sub, three geographically distributed VMs, and PostgreSQL.

---

## 👥 Team — Chunky/Sunny

| Name                 | Email            |
| -------------------- | ---------------- |
| Evelyn Nguyen        | evelynng@pdx.edu |
| Ryan Le              | ryle@pdx.edu     |
| Camellia Tran        | cametran@pdx.edu |
| Camila Beriztain Cox | camilab@pdx.edu  |

**GCP Project:** `chunky-dataeng` &nbsp;|&nbsp; **Vehicle Group:** Sunny &nbsp;|&nbsp; **Fleet size:** 222 vehicles

---

## 📁 Repository Structure

```
breadcrumb-pipeline/
├── part1/                      # Basic publish → subscribe pipeline
│   ├── publisher.py            # Fetches breadcrumbs from BusData API, publishes to bc_topic
│   ├── analysis.py             # Subscribes via analysis_sub, logs daily stats
│   ├── backup.py               # Subscribes via backup_sub, writes compressed daily log
│   └── systemd_and_cron.conf   # Crontab entry + systemd unit files for Part 1
│
├── part2/                      # Adds validation, transformation, and PostgreSQL loading
│   ├── analysis.py             # Updated: validates, transforms, batch-inserts to DB
│
│
└── part3/                      # Parallel StopEvent pipeline
    ├── se_publisher.py         # Fetches StopEvent HTML, parses with BeautifulSoup, publishes to se_topic
    ├── se_backup.py            # Subscribes via se_backup_sub, archives to .json.gz
    ├── se_analysis.py          # Validates, converts coordinates, loads into StopEvent table
    └── systemd_and_cron.conf   # Updated crontab + systemd unit files for Part 3
```

---

## 🏗️ Architecture

Two parallel pipelines share the same three GCP VMs and write into the same PostgreSQL database on the Analysis VM.

```
                        ┌─────────────────────────────────────────────────────┐
                        │               Publisher VM  (Oregon)                 │
                        │   publisher.py          se_publisher.py              │
                        └────────┬────────────────────────┬────────────────────┘
                                 │                        │
                           bc_topic                   se_topic
                          ┌──────┴──────┐           ┌──────┴──────┐
                    analysis_sub    backup_sub  se_analysis_sub  se_backup_sub
                          │              │           │                 │
              ┌───────────▼──────────────▼───────────▼─────────────────▼──────────┐
              │   Analysis VM (Belgium)         │        Backup VM (Taiwan)        │
              │   analysis.py  se_analysis.py  │   backup.py    se_backup.py      │
              │         │             │         │   breadcrumbs_DATE.log.gz        │
              │         ▼             ▼         │   se_DATE.json.gz                │
              │      PostgreSQL (breadcrumbs DB)│                                  │
              │   ┌──────────────────────────┐  │                                  │
              │   │  breadcrumb  │ StopEvent  │  │                                  │
              │   └──────────────────────────┘  │                                  │
              └─────────────────────────────────┴──────────────────────────────────┘
```

---

## Part 1 — Basic Pub/Sub Pipeline

The foundation: fetch → publish → subscribe → archive.

### How it works

**`publisher.py`** (runs daily via cron on the Publisher VM at 9:10 AM Pacific):

- Iterates over all 222 vehicle IDs
- Makes HTTP GET requests to `https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=<id>`
- Publishes each breadcrumb record individually as a JSON message to `bc_topic`
- Sends a **sentinel message** after all records are published, containing the exact count of published messages so subscribers don't stop early if the sentinel arrives out of order

**`analysis.py`** (runs continuously as a systemd service on the Analysis VM):

- Pulls messages from `analysis_sub` in a loop
- Tracks daily statistics: vehicle count, trip count, breadcrumb count, min/max timestamps, throughput
- Logs a summary and resets when the sentinel is received and the expected count is matched

**`backup.py`** (runs continuously as a systemd service on the Backup VM):

- Pulls messages from `backup_sub`
- Writes each record to a daily `.log` file (`breadcrumbs_YYYY-MM-DD.log`)
- Compresses the file to `.log.gz` on sentinel receipt and logs file size + throughput

### Sentinel Design

```json
{
  "message_type": "sentinel",
  "team": "Sunny",
  "expected_breadcrumbs": 494108,
  "sent_at": "2026-05-06T17:12:44+00:00"
}
```

Including `expected_breadcrumbs` in the sentinel means subscribers keep pulling until the count is satisfied — safely handling GCP Pub/Sub's out-of-order delivery guarantee.

### GCP Infrastructure

| Resource       | Type                            | Region                 |
| -------------- | ------------------------------- | ---------------------- |
| Publisher VM   | e2-medium, Debian 11            | us-west1 (Oregon)      |
| Analysis VM    | e2-medium, Debian 11            | europe-west1 (Belgium) |
| Backup VM      | e2-medium, Debian 11            | asia-east1 (Taiwan)    |
| `bc_topic`     | Pub/Sub Topic                   | —                      |
| `analysis_sub` | Pull subscription on `bc_topic` | —                      |
| `backup_sub`   | Pull subscription on `bc_topic` | —                      |

---

## Part 2 — Validation, Transformation, and Loading

`analysis.py` was extended to validate every incoming record, transform valid ones, and load them into PostgreSQL.

### Validation (7 assertions)

| #   | Assertion                                                    | Type                  | Action      |
| --- | ------------------------------------------------------------ | --------------------- | ----------- |
| A1  | `GPS_LATITUDE` non-null and in `[-90, 90]`                   | Limit                 | Drop record |
| A2  | `GPS_LONGITUDE` non-null and in `[-180, 180]`                | Limit                 | Drop record |
| A3  | `EVENT_NO_TRIP` must be present and non-null                 | Existence             | Drop record |
| A4  | `VEHICLE_ID` must be present and non-null                    | Existence             | Drop record |
| A5  | `METERS` must be `>= 0`                                      | Limit                 | Drop record |
| A9  | No duplicate `(EVENT_NO_TRIP, ACT_TIME)` pairs within a trip | Inter-record          | Drop record |
| A13 | Each `EVENT_NO_TRIP` must map to exactly one `VEHICLE_ID`    | Referential integrity | Log warning |

Invalid records are written to `invalid_data_YYYY-MM-DD.json` for review.

### Transformation Steps

1. **Remove fields** — drop `EVENT_NO_STOP`, `GPS_SATELLITES`, `GPS_HDOP`
2. **Build timestamp** — combine `OPD_DATE` + `ACT_TIME` (seconds since midnight) into a single `datetime`, then delete both source fields
3. **Compute speed** — `Δmeters / Δseconds` per trip; first breadcrumb of each trip gets `speed = 0.0`
4. **Rename fields** — align to database schema (`EVENT_NO_TRIP` → `trip_id`, etc.)

### PostgreSQL Schema — `breadcrumb`

```sql
CREATE TABLE IF NOT EXISTS breadcrumb (
    trip_id    INTEGER          NOT NULL,
    vehicle_id INTEGER          NOT NULL,
    timestamp  TIMESTAMP        NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL,
    speed      DOUBLE PRECISION,
    meters     INTEGER
);
```

### Pipeline Run Results (5 days)

| Pipeline Date | TriMet Date     | Breadcrumbs | Errors | Stored  | Vehicles | Throughput (msg/s) |
| ------------- | --------------- | ----------- | ------ | ------- | -------- | ------------------ |
| 2026-05-06    | 2023-01-07 → 08 | 494,108     | 191    | 493,917 | 114      | 1,379              |
| 2026-05-07    | 2023-01-08 → 09 | 500,779     | 618    | 500,161 | 114      | 852                |
| 2026-05-08    | 2023-01-09 → 10 | 740,681     | 747    | 739,934 | 158      | 1,013              |
| 2026-05-09    | 2023-01-10 → 11 | 749,762     | 460    | 749,302 | 163      | 1,346              |
| 2026-05-10    | 2023-01-11 → 12 | 687,623     | 315    | 687,308 | 152      | 1,388              |

**Total stored:** ~3.17 million breadcrumb records across 5 pipeline days, spanning TriMet data from January 6–12, 2023.

### Exploratory SQL Findings

- All **3,949,908** stored records fell within the Portland metro bounding box (lat 45–46, lon −123.5 to −122) — zero GPS outliers
- Average speed across all records: **8.69 m/s** (~19 mph), realistic for city bus service
- Only **5 records** exceeded 40 m/s (~90 mph) — physically impossible for a bus, flagged for follow-up
- Vehicle 3056 had the most breadcrumbs (36,598) across 70 trips over the dataset period

---

## Part 3 — StopEvent Pipeline

A second, parallel pipeline running alongside the BreadCrumb pipeline, adding passenger boarding/alighting data from TriMet's stop-event records.

### How it works

**`se_publisher.py`** (runs at 9:25 AM Pacific, 15 minutes after `publisher.py`):

- Fetches HTML pages from `https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num=<id>`
- Parses each page with **BeautifulSoup**, extracting one row per stop event per trip table
- Publishes each record as JSON to `se_topic`
- Sends a sentinel with `total_count` when done

**`se_backup.py`** (continuous systemd service on Backup VM):

- Archives daily records to `se_YYYY-MM-DD.json.gz`

**`se_analysis.py`** (continuous systemd service on Analysis VM):

- Validates, transforms, and loads records into the `StopEvent` PostgreSQL table

### Key Transformations

**Time conversion** — `arrive_time`, `leave_time`, and `stop_time` are stored as integer seconds past midnight. Each is converted to a full Python `datetime` by adding to midnight of the service date:

```python
datetime.combine(service_date, datetime.min.time()) + timedelta(seconds=arrive_time)
```

**Coordinate conversion** — `x_coordinate` / `y_coordinate` use the Oregon State Plane Coordinate System (EPSG:2913, international feet). Converted to WGS84 GPS lat/lon using `pyproj`:

```python
from pyproj import Transformer
transformer = Transformer.from_crs("EPSG:2913", "EPSG:4326", always_xy=True)
lon, lat = transformer.transform(x_coordinate, y_coordinate)
```

### Validation (8 assertions)

| #   | Assertion                                                 | Type         | Action      |
| --- | --------------------------------------------------------- | ------------ | ----------- |
| 1   | `arrive_time` non-null and in `[0, 108000]`               | Limit        | Drop record |
| 2   | `leave_time` non-null and `>= arrive_time`                | Intra-record | Drop record |
| 3   | `vehicle_number` non-null and positive                    | Existence    | Drop record |
| 4   | `trip_number` non-null and positive                       | Existence    | Drop record |
| 5   | `ons` and `offs` non-negative                             | Limit        | Drop record |
| 6   | No duplicate `(vehicle_number, trip_number, arrive_time)` | Inter-record | Drop record |
| 7   | GPS coordinates within Portland metro bounds              | Limit        | Drop record |
| 8   | `estimated_load >= ons - offs`                            | Intra-record | Log warning |

### PostgreSQL Schema — `StopEvent`

```sql
CREATE TABLE IF NOT EXISTS StopEvent (
    vehicle_number   INTEGER   NOT NULL,
    leave_time       TIMESTAMP,
    train            INTEGER,
    route_number     INTEGER,
    direction        SMALLINT,
    service_key      CHAR(1),
    trip_number      INTEGER   NOT NULL,
    stop_time        TIMESTAMP,
    arrive_time      TIMESTAMP NOT NULL,
    dwell            INTEGER,
    location_id      INTEGER,
    door             INTEGER,
    lift             INTEGER,
    ons              INTEGER,
    offs             INTEGER,
    estimated_load   INTEGER,
    maximum_speed    INTEGER,
    train_mileage    FLOAT,
    pattern_distance FLOAT,
    location_distance FLOAT,
    GPS_latitude     FLOAT,
    GPS_longitude    FLOAT,
    data_source      SMALLINT,
    schedule_status  SMALLINT,
    PRIMARY KEY (vehicle_number, trip_number, arrive_time)
);
```

### New Pub/Sub Resources (Part 3)

| Resource          | Type                            |
| ----------------- | ------------------------------- |
| `se_topic`        | Pub/Sub Topic                   |
| `se_analysis_sub` | Pull subscription on `se_topic` |
| `se_backup_sub`   | Pull subscription on `se_topic` |

---

## 🗺️ Data Visualization

Trip breadcrumbs were visualized using **Folium** with speed color-coding:

| Color            | Speed                    |
| ---------------- | ------------------------ |
| 🟢 Green         | > 10 m/s (fast)          |
| 🟡 Yellow/Orange | 3–10 m/s (normal)        |
| 🔴 Red           | < 3 m/s (slow / stopped) |

Example: Trip `233761856` (980 breadcrumbs, Jan 7 2023) travels east–west across the Portland metro. Green markers appear along long straight segments; red and orange cluster near intersections, traffic signals, and bus stops — exactly the expected pattern for urban transit.

The `BreadCrumb` and `StopEvent` tables are joined on `trip_number` / `EVENT_NO_TRIP` to overlay GPS breadcrumb trails with passenger boarding locations on the same map.

---

## ⚙️ Dependencies

```
google-cloud-pubsub
psycopg2-binary
requests
beautifulsoup4
pyproj
folium
pandas
```

Install on each VM:

```bash
pip install google-cloud-pubsub psycopg2-binary requests beautifulsoup4 pyproj
```

---

## 🚀 Running the Pipeline

### First-time setup on each VM

```bash
# Set timezone to Pacific
sudo timedatectl set-timezone America/Los_Angeles

# Install dependencies
pip install google-cloud-pubsub psycopg2-binary requests beautifulsoup4 pyproj

# Copy programs to home directory
scp publisher.py analysis.py backup.py <VM_IP>:~/
```

### Publisher VM — schedule via cron

```bash
crontab -e
# Add:
10 9 * * * /usr/bin/python3 /home/cametran/publisher.py >> /home/cametran/publisher.log 2>&1
25 9 * * * /home/cametran/venv/bin/python /home/cametran/se_publisher.py >> /home/cametran/se_publisher.log 2>&1
```

### Analysis & Backup VMs — install systemd services

```bash
sudo cp analysis.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable analysis.service
sudo systemctl start analysis.service
sudo systemctl status analysis.service
```

### Verify data is flowing

```bash
# On any VM — tail the log
tail -f /home/cametran/analysis.log

# In Colab / psql — quick row count check
SELECT COUNT(*) FROM breadcrumb;
SELECT COUNT(*) FROM StopEvent;
```

---

## 📊 Database Quick Reference

```sql
-- Total records per day
SELECT DATE(timestamp) AS date, COUNT(*) AS breadcrumbs,
       COUNT(DISTINCT vehicle_id) AS vehicles,
       COUNT(DISTINCT trip_id) AS trips
FROM breadcrumb
GROUP BY DATE(timestamp)
ORDER BY date;

-- Top routes by boardings (requires StopEvent)
SELECT route_number, SUM(ons) AS total_boardings, COUNT(*) AS stop_events
FROM StopEvent
GROUP BY route_number
ORDER BY total_boardings DESC
LIMIT 10;

-- Average speed per route for high-boarding trips (join)
SELECT se.route_number,
       ROUND(AVG(bc.speed)::numeric, 2) AS avg_speed_mps,
       COUNT(DISTINCT bc.trip_id) AS qualifying_trips
FROM breadcrumb bc
JOIN StopEvent se ON bc.trip_id = se.trip_number
GROUP BY se.route_number
HAVING SUM(se.ons) >= 10
ORDER BY avg_speed_mps DESC;
```
