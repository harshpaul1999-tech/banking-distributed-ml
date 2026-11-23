"""Create deterministic, timestamped CSV chunks from bank.csv for replay."""
import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

EXTRA_FIELDS = ["event_id", "event_time"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/bank.csv")
    parser.add_argument("--output-dir", default="data/stream_sample")
    parser.add_argument("--chunk-size", type=int, default=30)
    parser.add_argument("--rows", type=int, default=120,
                        help="0 means use every source row")
    parser.add_argument("--step-seconds", type=int, default=5)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(args.input, newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        fields = list(reader.fieldnames or [])
        rows = list(reader)
    if args.rows > 0:
        rows = rows[:args.rows]
    start = datetime(2025, 5, 1, 9, 0, 0)
    enriched = []
    for index, row in enumerate(rows):
        event_time = start + timedelta(seconds=args.step_seconds * index)
        # Every 17th row arrives with a 20-second-old event time. With the
        # demo's 1-minute watermark this is late but still inside the allowance.
        if index > 0 and index % 17 == 0:
            event_time -= timedelta(seconds=20)
        row["event_id"] = str(index)
        row["event_time"] = event_time.strftime("%Y-%m-%d %H:%M:%S")
        enriched.append(row)

    for path in output_dir.glob("chunk-*.csv"):
        path.unlink()
    output_fields = fields + EXTRA_FIELDS
    for chunk_index in range(0, len(enriched), args.chunk_size):
        chunk = enriched[chunk_index:chunk_index + args.chunk_size]
        path = output_dir / f"chunk-{chunk_index // args.chunk_size:04d}.csv"
        with open(path, "w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=output_fields)
            writer.writeheader()
            writer.writerows(chunk)
        print(f"{path}: {len(chunk)} records")
    print(f"created {len(enriched)} records in {output_dir}")
    print("event_time is synthetic replay metadata; source data contains no event timestamp.")


if __name__ == "__main__":
    main()
