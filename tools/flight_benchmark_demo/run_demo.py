#!/usr/bin/env python3

import argparse
import csv
import json
import random
import re
import sqlite3
import struct
import subprocess
import time
from pathlib import Path


STREAMS = {
    "IMU": 100,
    "GPS": 10,
    "BATTERY": 1,
    "CAMERA_METADATA": 30,
}


def ensure_clean_output(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    for path in output_dir.rglob("*"):
        if path.is_file():
            path.unlink()

    data_dir.mkdir(parents=True, exist_ok=True)


def generate_payload(stream_type: str, payload_bytes: int) -> bytes:
    if stream_type == "IMU":
        values = [
            random.randint(-2000, 2000),
            random.randint(-2000, 2000),
            random.randint(-2000, 2000),
            random.randint(-500, 500),
            random.randint(-500, 500),
            random.randint(-500, 500),
        ]
    elif stream_type == "GPS":
        values = [
            random.randint(410000000, 420000000),
            random.randint(280000000, 300000000),
            random.randint(0, 200000),
            random.randint(0, 3000),
        ]
    elif stream_type == "BATTERY":
        values = [
            random.randint(11000, 12600),
            random.randint(0, 60000),
            random.randint(0, 100),
        ]
    else:
        values = [
            random.randint(0, 10_000_000),
            random.randint(0, 3840),
            random.randint(0, 2160),
            random.randint(0, 100),
        ]

    raw = b"".join(struct.pack("<i", value) for value in values)

    if len(raw) >= payload_bytes:
        return raw[:payload_bytes]

    return raw + bytes(payload_bytes - len(raw))


def generate_telemetry(records: int, payload_bytes: int):
    stream_names = list(STREAMS.keys())
    weights = list(STREAMS.values())

    base_timestamp_ns = 1_700_000_000_000_000_000
    timestamp_ns = base_timestamp_ns

    telemetry = []

    for index in range(records):
        stream_type = random.choices(stream_names, weights=weights, k=1)[0]

        timestamp_ns += random.randint(5_000_000, 15_000_000)
        payload = generate_payload(stream_type, payload_bytes)

        telemetry.append({
            "id": index,
            "timestamp_ns": timestamp_ns,
            "stream_type": stream_type,
            "payload": payload,
        })

    return telemetry


def measure_csv_write(telemetry, output_path: Path):
    start = time.perf_counter()

    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "timestamp_ns", "stream_type", "payload_hex"])

        for record in telemetry:
            writer.writerow([
                record["id"],
                record["timestamp_ns"],
                record["stream_type"],
                record["payload"].hex(),
            ])

    write_seconds = time.perf_counter() - start

    query_start = time.perf_counter()
    readback_count = 0

    with output_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["stream_type"] == "GPS":
                readback_count += 1

    query_latency_ms = (time.perf_counter() - query_start) * 1000.0

    return {
        "name": "CSV",
        "write_seconds": write_seconds,
        "records_per_second": len(telemetry) / write_seconds if write_seconds > 0 else 0,
        "disk_usage_bytes": output_path.stat().st_size,
        "query_latency_ms": query_latency_ms,
        "readback_count": readback_count,
        "correctness": readback_count > 0,
        "compression_ratio": None,
    }


def measure_sqlite_write(telemetry, output_path: Path):
    start = time.perf_counter()

    conn = sqlite3.connect(output_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE telemetry (
            id INTEGER PRIMARY KEY,
            timestamp_ns INTEGER NOT NULL,
            stream_type TEXT NOT NULL,
            payload BLOB NOT NULL
        )
    """)

    rows = [
        (
            record["id"],
            record["timestamp_ns"],
            record["stream_type"],
            record["payload"],
        )
        for record in telemetry
    ]

    cursor.executemany(
        "INSERT INTO telemetry (id, timestamp_ns, stream_type, payload) VALUES (?, ?, ?, ?)",
        rows,
    )

    conn.commit()
    write_seconds = time.perf_counter() - start

    query_start = time.perf_counter()
    cursor.execute("SELECT COUNT(*) FROM telemetry WHERE stream_type = ?", ("GPS",))
    readback_count = cursor.fetchone()[0]
    query_latency_ms = (time.perf_counter() - query_start) * 1000.0

    conn.close()

    return {
        "name": "SQLite",
        "write_seconds": write_seconds,
        "records_per_second": len(telemetry) / write_seconds if write_seconds > 0 else 0,
        "disk_usage_bytes": output_path.stat().st_size,
        "query_latency_ms": query_latency_ms,
        "readback_count": readback_count,
        "correctness": readback_count > 0,
        "compression_ratio": None,
    }


def measure_raw_binary_write(telemetry, output_path: Path):
    stream_ids = {
        "IMU": 1,
        "GPS": 2,
        "BATTERY": 3,
        "CAMERA_METADATA": 4,
    }

    start = time.perf_counter()

    with output_path.open("wb") as f:
        for record in telemetry:
            payload = record["payload"]
            header = struct.pack(
                "<QHH",
                record["timestamp_ns"],
                stream_ids[record["stream_type"]],
                len(payload),
            )
            f.write(header)
            f.write(payload)

    write_seconds = time.perf_counter() - start

    query_start = time.perf_counter()
    readback_count = 0

    with output_path.open("rb") as f:
        while True:
            header = f.read(12)
            if not header:
                break

            timestamp_ns, stream_id, payload_size = struct.unpack("<QHH", header)
            payload = f.read(payload_size)

            if stream_id == 2:
                readback_count += 1

    query_latency_ms = (time.perf_counter() - query_start) * 1000.0

    return {
        "name": "Raw Binary",
        "write_seconds": write_seconds,
        "records_per_second": len(telemetry) / write_seconds if write_seconds > 0 else 0,
        "disk_usage_bytes": output_path.stat().st_size,
        "query_latency_ms": query_latency_ms,
        "readback_count": readback_count,
        "correctness": readback_count > 0,
        "compression_ratio": None,
    }

def parse_benchmark_output(stdout: str):
    metrics = {}

    for line in stdout.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        try:
            if "." in value:
                metrics[key] = float(value)
            else:
                metrics[key] = int(value)
        except ValueError:
            metrics[key] = value

    return metrics


def measure_edgestorage_benchmark(records: int, payload_bytes: int):
    benchmark_path = Path("build") / "edgestorage_benchmark"

    if not benchmark_path.exists():
        return {
            "name": "EdgeStorage",
            "write_seconds": None,
            "records_per_second": 0,
            "disk_usage_bytes": 0,
            "query_latency_ms": 0,
            "readback_count": 0,
            "correctness": False,
            "compression_ratio": None,
            "note": "build/edgestorage_benchmark not found"
        }

    command = [
        str(benchmark_path),
        str(records),
        "100",
        str(payload_bytes),
        "mixed_drone_i32",
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        return {
            "name": "EdgeStorage",
            "write_seconds": None,
            "records_per_second": 0,
            "disk_usage_bytes": 0,
            "query_latency_ms": 0,
            "readback_count": 0,
            "correctness": False,
            "compression_ratio": None,
            "note": completed.stderr.strip() or "edgestorage_benchmark failed"
        }

    metrics = parse_benchmark_output(completed.stdout)

    write_seconds = float(metrics.get("compressed_write_seconds", 0))
    records_per_second = float(metrics.get("compressed_write_records_per_sec", 0))
    disk_usage_bytes = int(metrics.get("compressed_size_bytes", 0))
    query_latency_ms = float(metrics.get("query_latency_ms", 0))
    readback_count = int(metrics.get("query_result_count", 0))
    compression_ratio = float(metrics.get("compression_ratio", 0))
    uncompressed_size_bytes = int(metrics.get("uncompressed_size_bytes", 0))
    compressed_size_bytes = int(metrics.get("compressed_size_bytes", 0))

    return {
        "name": "EdgeStorage",
        "write_seconds": write_seconds,
        "records_per_second": records_per_second,
        "disk_usage_bytes": disk_usage_bytes,
        "query_latency_ms": query_latency_ms,
        "readback_count": readback_count,
        "correctness": readback_count == records,
        "compression_ratio": compression_ratio,
        "uncompressed_size_bytes": uncompressed_size_bytes,
        "compressed_size_bytes": compressed_size_bytes,
        "note": "Measured via native C benchmark executable (compressed write path).",
    }

def parse_benchmark_output(stdout: str):
    metrics = {}

    for line in stdout.splitlines():
        match = re.search(r"([A-Za-z0-9_]+)\s*[:=]\s*([0-9.]+)", line)
        if match:
            key = match.group(1)
            value = float(match.group(2))
            metrics[key] = value

    return metrics


def first_existing_metric(metrics, keys, default=None):
    for key in keys:
        if key in metrics:
            return metrics[key]
    return default


def measure_edgestorage_benchmark(records: int, payload_bytes: int):
    benchmark_path = Path("build") / "edgestorage_benchmark"

    if not benchmark_path.exists():
        return {
            "name": "EdgeStorage",
            "write_seconds": None,
            "records_per_second": 0,
            "disk_usage_bytes": 0,
            "query_latency_ms": 0,
            "readback_count": 0,
            "correctness": False,
            "note": "build/edgestorage_benchmark not found. Run CMake build first."
        }

    command = [
        str(benchmark_path),
        str(records),
        "100",
        str(payload_bytes),
        "mixed_drone_i32",
    ]

    start = time.perf_counter()

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    total_seconds = time.perf_counter() - start

    if completed.returncode != 0:
        return {
            "name": "EdgeStorage",
            "write_seconds": total_seconds,
            "records_per_second": 0,
            "disk_usage_bytes": 0,
            "query_latency_ms": 0,
            "readback_count": 0,
            "correctness": False,
            "note": completed.stderr.strip() or "edgestorage_benchmark failed."
        }

    metrics = parse_benchmark_output(completed.stdout)

    records_per_second = first_existing_metric(metrics, [
        "compressed_records_per_sec",
        "compressed_write_records_per_sec",
        "batch_records_per_sec",
        "batch_write_records_per_sec",
        "records_per_sec",
    ], default=0)

    write_seconds = first_existing_metric(metrics, [
        "compressed_write_seconds",
        "batch_write_seconds",
        "write_seconds",
    ], default=total_seconds)

    query_latency_ms = first_existing_metric(metrics, [
        "query_latency_ms",
        "read_query_latency_ms",
    ], default=0)

    readback_count = first_existing_metric(metrics, [
        "query_result_count",
        "readback_count",
        "records_read",
    ], default=0)

    uncompressed_size = first_existing_metric(metrics, [
        "uncompressed_size_bytes",
        "uncompressed_bytes",
    ], default=0)

    compressed_size = first_existing_metric(metrics, [
        "compressed_size_bytes",
        "compressed_bytes",
    ], default=0)

    compression_ratio = first_existing_metric(metrics, [
        "compression_ratio",
    ], default=None)

    disk_usage_bytes = compressed_size if compressed_size else 0

    return {
        "name": "EdgeStorage",
        "write_seconds": write_seconds,
        "records_per_second": records_per_second,
        "disk_usage_bytes": disk_usage_bytes,
        "query_latency_ms": query_latency_ms,
        "readback_count": int(readback_count),
        "correctness": int(readback_count) == records if readback_count else False,
        "compression_ratio": compression_ratio,
        "uncompressed_size_bytes": uncompressed_size,
        "compressed_size_bytes": compressed_size,
        "benchmark_stdout": completed.stdout,
    }

def format_bytes(value: int) -> str:
    mb = value / (1024 * 1024)
    return f"{mb:.2f} MB"


def generate_html_report(summary, output_path: Path):
    rows = ""

    for result in summary["results"]:
        compression_ratio = result.get("compression_ratio")
        compression_ratio_display = f'{compression_ratio:.3f}x' if compression_ratio else "-"

        rows += f"""
        <tr>
            <td>{result["name"]}</td>
            <td>{result["records_per_second"]:,.0f} rec/s</td>
            <td>{format_bytes(result["disk_usage_bytes"])}</td>
            <td>{compression_ratio_display}</td>
            <td>{result["query_latency_ms"]:.3f} ms</td>
            <td>{"OK" if result["correctness"] else "FAILED"}</td>
        </tr>
        """

    html = f"""<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>EdgeStorage Flight Benchmark Demo</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 40px;
            background: #f6f7f9;
            color: #171717;
        }}
        .container {{
            max-width: 1100px;
            margin: auto;
        }}
        .hero {{
            background: white;
            padding: 32px;
            border-radius: 18px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.06);
            margin-bottom: 24px;
        }}
        .cards {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: white;
            padding: 20px;
            border-radius: 16px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.05);
        }}
        .card .label {{
            color: #666;
            font-size: 13px;
            margin-bottom: 8px;
        }}
        .card .value {{
            font-size: 24px;
            font-weight: 700;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 8px 30px rgba(0,0,0,0.05);
        }}
        th, td {{
            padding: 16px;
            border-bottom: 1px solid #eee;
            text-align: left;
        }}
        th {{
            background: #111827;
            color: white;
        }}
        .note {{
            background: #fff;
            padding: 24px;
            border-radius: 16px;
            margin-top: 24px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.05);
            line-height: 1.6;
        }}
        code {{
            background: #eef0f3;
            padding: 2px 6px;
            border-radius: 6px;
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="hero">
        <h1>EdgeStorage Flight Benchmark Demo</h1>
        <p>
            Same flight-like telemetry workload, stored with multiple local storage methods.
            This demo measures write throughput, disk usage, query latency, and readback correctness.
        </p>
    </div>

    <div class="cards">
        <div class="card">
            <div class="label">Records</div>
            <div class="value">{summary["workload"]["records"]:,}</div>
        </div>
        <div class="card">
            <div class="label">Payload</div>
            <div class="value">{summary["workload"]["payload_bytes"]} B</div>
        </div>
        <div class="card">
            <div class="label">Streams</div>
            <div class="value">4</div>
        </div>
        <div class="card">
            <div class="label">Methods</div>
            <div class="value">{len(summary["results"])}</div>
        </div>
    </div>

    <table>
        <thead>
            <tr>
                <th>Storage Method</th>
                <th>Write Throughput</th>
                <th>Disk Usage</th>
                <th>Compression Ratio</th>
                <th>Query Latency</th>
                <th>Readback</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>

    <div class="note">
        <h2>What this MVP proves</h2>
        <p>
            This benchmark demo is designed to show a narrow, honest MVP scope:
            telemetry-like records can be generated, written to different local formats,
            measured, read back, and compared under the same workload.
        </p>
        <p>
            The next step is to connect this visual benchmark wrapper to the existing
            <code>edgestorage_benchmark</code> executable, so EdgeStorage appears next to
            SQLite, CSV, and raw binary in the same report.
        </p>
    </div>
</div>
</body>
</html>
"""

    output_path.write_text(html)


def main():
    parser = argparse.ArgumentParser(description="EdgeStorage Flight Benchmark Demo")
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--output-dir", type=str, default="demo_output")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    data_dir = output_dir / "data"

    ensure_clean_output(output_dir)

    print("Generating telemetry workload...")
    telemetry = generate_telemetry(args.records, args.payload_bytes)

    print("Measuring CSV...")
    csv_result = measure_csv_write(telemetry, data_dir / "telemetry.csv")

    print("Measuring SQLite...")
    sqlite_result = measure_sqlite_write(telemetry, data_dir / "telemetry.sqlite")

    print("Measuring raw binary...")
    raw_binary_result = measure_raw_binary_write(telemetry, data_dir / "telemetry.bin")

    print("Measuring EdgeStorage...")
    edgestorage_result = measure_edgestorage_benchmark(args.records, args.payload_bytes)

    summary = {
        "demo": "EdgeStorage Flight Benchmark Demo",
        "workload": {
            "records": args.records,
            "payload_bytes": args.payload_bytes,
            "streams": list(STREAMS.keys()),
        },
        "results": [
            edgestorage_result,
            sqlite_result,
            csv_result,
            raw_binary_result,
        ],
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "demo_report.html"

    summary_path.write_text(json.dumps(summary, indent=2))
    generate_html_report(summary, report_path)

    print()
    print("Demo complete.")
    print(f"Summary: {summary_path}")
    print(f"HTML report: {report_path}")


if __name__ == "__main__":
    main()