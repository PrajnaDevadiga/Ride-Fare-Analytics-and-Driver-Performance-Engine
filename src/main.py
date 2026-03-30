from __future__ import annotations

import argparse
import os
import sys
import pandas as pd
from .engine import run_pipeline


def parse_args(argv=None):
	parser = argparse.ArgumentParser(description="Ride Fare Analytics & Driver Performance Engine")
	parser.add_argument("--drivers", required=True, help="Path to drivers.csv")
	parser.add_argument("--rides", required=True, help="Path to rides.csv")
	parser.add_argument("--outdir", default=".", help="Output directory for CSVs")
	return parser.parse_args(argv)


def main(argv=None):
	args = parse_args(argv)
	performance_df, anomalies_df = run_pipeline(args.drivers, args.rides)

	os.makedirs(args.outdir, exist_ok=True)
	driver_perf_path = os.path.join(args.outdir, "driver_performance.csv")
	anomaly_report_path = os.path.join(args.outdir, "anomaly_report.csv")

	performance_df.to_csv(driver_perf_path, index=False)
	anomalies_df.to_csv(anomaly_report_path, index=False)

	# Print summary to stdout for quick feedback
	print(f"Wrote: {driver_perf_path} ({len(performance_df)} rows)")
	print(f"Wrote: {anomaly_report_path} ({len(anomalies_df)} rows)")


if __name__ == "__main__":
	sys.exit(main())

