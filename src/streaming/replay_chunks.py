"""Copy prepared CSV chunks one at a time into a Structured Streaming inbox."""
import argparse
import shutil
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/stream_sample")
    parser.add_argument("--inbox", default="/tmp/bank_stream_inbox")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    source = Path(args.source)
    inbox = Path(args.inbox)
    inbox.mkdir(parents=True, exist_ok=True)
    for stale in inbox.glob("chunk-*.csv"):
        stale.unlink()
    chunks = sorted(source.glob("chunk-*.csv"))
    for chunk in chunks:
        destination = inbox / chunk.name
        shutil.copy2(chunk, destination)
        print(f"published {destination}", flush=True)
        time.sleep(args.interval)
    print(f"replayed {len(chunks)} chunks")


if __name__ == "__main__":
    main()
