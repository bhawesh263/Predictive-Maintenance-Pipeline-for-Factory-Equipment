# Spins up a handful of virtual machines that each push telemetry readings
# through the local IoT Hub emulator on their own thread. Same reading format
# as azure/simulator/sensor_simulator_azure.py - this one just doesn't need a
# real Azure subscription to run.
#
# python sensor_simulator.py --machines 6 --duration 120 --interval 1.5

import argparse
import random
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.iot_hub_emulator import DeviceRegistry, IoTHubClient

ANOMALY_RATE = 0.05     # bumped up from a "real" ~1-3% so anomalies show up in short demo runs
BAD_SCHEMA_RATE = 0.02  # occasionally drop a field, to give the validation gate something to catch


def generate_reading(machine_id: str, inject_anomaly: bool = False, inject_bad_schema: bool = False):
    temp = random.uniform(60, 75)
    vibration = random.uniform(0.2, 0.8)

    if inject_anomaly:
        temp += random.uniform(15, 30)
        vibration += random.uniform(1.5, 3.0)

    reading = {
        "machine_id": machine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temp, 2),
        "vibration": round(vibration, 3),
        "rpm": random.randint(1200, 1800),
        "status": "warning" if inject_anomaly else "running",
    }

    if inject_bad_schema:
        del reading["vibration"]

    return reading


def next_sleep_interval(interval: float) -> float:
    """Add a bit of jitter so machines don't all report in lockstep, without ever going negative."""
    jitter = random.uniform(-0.3, 0.3)
    return max(0.1, interval + jitter)


def run_device(client: IoTHubClient, machine_id: str, duration: float, interval: float, stop_event: threading.Event):
    client.connect()
    print(f"[{machine_id}] connected")
    end_time = time.time() + duration if duration else None

    try:
        while not stop_event.is_set():
            if end_time and time.time() > end_time:
                break

            anomaly = random.random() < ANOMALY_RATE
            bad_schema = random.random() < BAD_SCHEMA_RATE

            reading = generate_reading(machine_id, inject_anomaly=anomaly, inject_bad_schema=bad_schema)
            client.send_message(reading)

            tag = "ANOMALY" if anomaly else ("BAD_SCHEMA" if bad_schema else "ok")
            print(f"[{machine_id}] sent ({tag}): {reading}")

            time.sleep(next_sleep_interval(interval))
    finally:
        client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Simulate factory machine telemetry over a local IoT Hub emulator.")
    parser.add_argument("--machines", type=int, default=6, help="number of virtual machines (devices)")
    parser.add_argument("--duration", type=float, default=90, help="seconds to run (0 = run forever, Ctrl+C to stop)")
    parser.add_argument("--interval", type=float, default=1.5, help="avg seconds between readings per machine")
    args = parser.parse_args()

    registry = DeviceRegistry()
    stop_event = threading.Event()
    threads = []

    for i in range(1, args.machines + 1):
        machine_id = f"machine-{i:02d}"
        registry.register(machine_id)
        client = IoTHubClient(machine_id, registry)
        t = threading.Thread(target=run_device, args=(client, machine_id, args.duration, args.interval, stop_event), daemon=True)
        threads.append(t)
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop_event.set()
        print("\nStopping simulator...")


if __name__ == "__main__":
    main()
