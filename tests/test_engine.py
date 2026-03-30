import pandas as pd
from src.engine import validate_drivers, validate_rides, detect_anomalies, compute_driver_performance


def make_drivers():
	return pd.DataFrame(
		[
			{"driver_id": "d1", "status": "ACTIVE"},
			{"driver_id": "d2", "status": "BLOCKED"},
			{"driver_id": None, "status": "ACTIVE"},
		]
	)


def make_rides():
	return pd.DataFrame(
		[
			# Valid for d1
			{"driver_id": "d1", "fare_amount": 100, "ride_time": "2024-01-01T10:00:00Z", "ride_status": "COMPLETED"},
			# Negative fare
			{"driver_id": "d1", "fare_amount": -5, "ride_time": "2024-01-01T11:00:00Z", "ride_status": "COMPLETED"},
			# High fare
			{"driver_id": "d1", "fare_amount": 600, "ride_time": "2024-01-01T12:00:00Z", "ride_status": "COMPLETED"},
			# Invalid time
			{"driver_id": "d1", "fare_amount": 50, "ride_time": "invalid", "ride_status": "COMPLETED"},
			# Not completed
			{"driver_id": "d1", "fare_amount": 40, "ride_time": "2024-01-01T13:00:00Z", "ride_status": "CANCELLED"},
			# Driver blocked (d2) but present in rides
			{"driver_id": "d2", "fare_amount": 30, "ride_time": "2024-01-01T14:00:00Z", "ride_status": "COMPLETED"},
			# For rapid rides test (3 within 2 minutes)
			{"driver_id": "d1", "fare_amount": 10, "ride_time": "2024-01-01T15:00:00Z", "ride_status": "COMPLETED"},
			{"driver_id": "d1", "fare_amount": 10, "ride_time": "2024-01-01T15:01:00Z", "ride_status": "COMPLETED"},
			{"driver_id": "d1", "fare_amount": 10, "ride_time": "2024-01-01T15:01:30Z", "ride_status": "COMPLETED"},
		]
	)


def test_invalid_driver_rejected():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	# Ensure no rides from missing driver ids (None) and no rides from drivers not in valid list
	assert valid_rides["driver_id"].isin(valid_drivers["driver_id"]).all()


def test_blocked_driver_rejected():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	# d2 is blocked, so only d1 remains
	assert set(valid_drivers["driver_id"]) == {"d1"}
	valid_rides = validate_rides(rides, valid_drivers)
	# No rides from d2 should remain
	assert (valid_rides["driver_id"] == "d2").sum() == 0


def test_negative_fare_ignored():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	# Negative fare ride removed
	assert (valid_rides["fare_amount"] <= 0).sum() == 0


def test_high_fare_flag():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	anoms = detect_anomalies(valid_rides)
	# There should be at least one HIGH_FARE
	assert "HIGH_FARE" in set(anoms["anomaly_type"])
	high_fares = anoms[anoms["anomaly_type"] == "HIGH_FARE"]
	assert (pd.to_numeric(high_fares["fare_amount"]) > 500).all()


def test_rapid_rides_flag():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	anoms = detect_anomalies(valid_rides)
	rapid = anoms[anoms["anomaly_type"] == "RAPID_RIDES"]
	# We created 3 rides within 2 minutes for d1 at 15:00, 15:01, 15:01:30
	assert not rapid.empty
	assert set(rapid["driver_id"]) == {"d1"}


def test_driver_earnings_calculation():
	drivers = make_drivers()
	rides = make_rides()
	valid_drivers = validate_drivers(drivers)
	valid_rides = validate_rides(rides, valid_drivers)
	perf = compute_driver_performance(valid_rides)
	# For d1, valid completed rides with positive fares: 100, 600, 10, 10, 10
	expected_total = 100 + 600 + 10 + 10 + 10
	row = perf[perf["driver_id"] == "d1"].iloc[0]
	assert int(row["total_rides"]) == 5
	assert float(row["total_earnings"]) == expected_total
	assert round(float(row["avg_fare"]), 2) == round(expected_total / 5, 2)

