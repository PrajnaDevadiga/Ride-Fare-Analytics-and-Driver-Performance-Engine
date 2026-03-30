from __future__ import annotations

import pandas as pd
from typing import Tuple, List


ACTIVE_STATUS = "ACTIVE"
BLOCKED_STATUS = "BLOCKED"
COMPLETED_STATUS = "COMPLETED"


def load_data(drivers_csv_path: str, rides_csv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
	drivers = pd.read_csv(drivers_csv_path)
	rides = pd.read_csv(rides_csv_path)
	return drivers, rides


def validate_drivers(drivers: pd.DataFrame) -> pd.DataFrame:
	df = drivers.copy()
	if "driver_id" not in df.columns:
		return df.iloc[0:0].copy()
	df = df.dropna(subset=["driver_id"])
	if "status" in df.columns:
		df = df[df["status"] == ACTIVE_STATUS]
	return df


def _parse_ride_time(rides: pd.DataFrame) -> pd.DataFrame:
	df = rides.copy()
	df["ride_time_parsed"] = pd.to_datetime(df.get("ride_time"), errors="coerce", utc=True)
	return df


def validate_rides(rides: pd.DataFrame, valid_drivers: pd.DataFrame) -> pd.DataFrame:
	df = _parse_ride_time(rides)
	required_cols: List[str] = ["driver_id", "fare_amount", "ride_status"]
	for col in required_cols:
		if col not in df.columns:
			return df.iloc[0:0].copy()

	# Filter by valid driver IDs
	valid_driver_ids = set(valid_drivers["driver_id"].unique())
	df = df[df["driver_id"].isin(valid_driver_ids)]

	# Fare must be positive
	df = df[pd.to_numeric(df["fare_amount"], errors="coerce") > 0]

	# Valid parsed time
	df = df[~df["ride_time_parsed"].isna()]

	# Completed rides only
	df = df[df["ride_status"] == COMPLETED_STATUS]

	return df.reset_index(drop=True)


def detect_anomalies(valid_completed_rides: pd.DataFrame) -> pd.DataFrame:
	if valid_completed_rides.empty:
		return pd.DataFrame(columns=["driver_id", "ride_time", "fare_amount", "anomaly_type"])

	df = valid_completed_rides.copy()

	# High fare anomalies
	high_fare_mask = pd.to_numeric(df["fare_amount"], errors="coerce") > 500
	high_fare = df.loc[high_fare_mask, ["driver_id", "ride_time", "fare_amount"]].copy()
	high_fare["anomaly_type"] = "HIGH_FARE"

	# Rapid rides anomalies (>2 within 2 minutes for a driver)
	rapid_flags = _flag_rapid_rides(df)
	rapid = df.loc[rapid_flags, ["driver_id", "ride_time", "fare_amount"]].copy()
	rapid["anomaly_type"] = "RAPID_RIDES"

	anomalies = pd.concat([high_fare, rapid], ignore_index=True)
	return anomalies.sort_values(by=["driver_id", "ride_time", "anomaly_type"]).reset_index(drop=True)


def _flag_rapid_rides(df: pd.DataFrame) -> pd.Series:
	# Expect df contains ride_time_parsed (from validate_rides)
	if "ride_time_parsed" not in df.columns:
		df = _parse_ride_time(df)
	flags = pd.Series(False, index=df.index)
	for driver_id, g in df.groupby("driver_id"):
		g_sorted = g.sort_values("ride_time_parsed")
		idx = g_sorted.index.to_list()
		times = g_sorted["ride_time_parsed"].to_list()
		start = 0
		n = len(times)
		while start < n:
			end = start
			# Expand end to include times within 2 minutes window
			while end < n and (times[end] - times[start]).total_seconds() <= 120:
				end += 1
			window_count = end - start
			if window_count >= 3:
				flags.loc[idx[start:end]] = True
			start += 1
	return flags


def compute_driver_performance(valid_completed_rides: pd.DataFrame) -> pd.DataFrame:
	if valid_completed_rides.empty:
		return pd.DataFrame(columns=["driver_id", "total_rides", "total_earnings", "avg_fare"])
	df = valid_completed_rides.copy()
	df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
	grouped = df.groupby("driver_id", as_index=False).agg(
		total_rides=("driver_id", "count"),
		total_earnings=("fare_amount", "sum"),
		avg_fare=("fare_amount", "mean"),
	)
	return grouped.sort_values(by=["total_earnings", "driver_id"], ascending=[False, True]).reset_index(drop=True)


def run_pipeline(drivers_csv_path: str, rides_csv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
	drivers, rides = load_data(drivers_csv_path, rides_csv_path)
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	anomalies = detect_anomalies(valid_rides)
	performance = compute_driver_performance(valid_rides)
	return performance, anomalies

