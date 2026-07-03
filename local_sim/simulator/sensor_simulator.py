"""
sensor_simulator.py (local version)

Same event model as azure/simulator/sensor_simulator_azure.py, but sends readings
through the local iot_hub_emulator instead of a real IoT Hub. Run this to generate
a stream of machine telemetry you can feed through the rest of the pipeline.

Usage:
    python sensor_simulator.py --machines 6 --duration 120 --interval 1.5
"""
import argparse
import random
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.iot_hub_emulator import DeviceRegistry, IoTHubClient


def generate_reading(machine_id: str, inject_anomaly: bool = False, inject_bad_schema: bool = False):
    base_temp = random.uniform(60, 75)
    base_vibration = random.uniform(0.2, 0.8)

    if inject_anomaly:
        base_temp += random.uniform(15, 30)
        base_vibration += random.uniform(1.5, 3.0)

    reading = {
        "machine_id": machine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(base_temp, 2),
        "vibration": round(base_vibration, 3),
        "rpm": random.randint(1200, 1800),
        "status": "running" if not inject_anomaly else "warning",
    }

    if inject_bad_schema:
        # simulate a malformed/missing-field event to test the ADF-style validation gate
        del reading["vibration"]

    return reading


def run_device(client: IoTHubClient, machine_id: str, duration: float, interval: float, stop_event: threading.Event):
    client.connect()
    print(f"[{machine_id}] connected")
    end_time = time.time() + duration if duration else None

    try:
        while not stop_event.is_set():
            if end_time and time.time() > end_time:
                break
            anomaly = random.random() < 0.05        # ~5% chance of anomaly (higher than prod for demo visibility)
            bad_schema = random.random() < 0.02      # ~2% chance of malformed event

            reading = generate_reading(machine_id, inject_anomaly=anomaly, inject_bad_schema=bad_schema)
            client.send_message(reading)
            tag = "ANOMALY" if anomaly else ("BAD_SCHEMA" if bad_schema else "ok")
            print(f"[{machine_id}] sent ({tag}): {reading}")

            time.sleep(interval + random.uniform(-0.3, 0.3) if interval > 0.3 else interval)
    finally:
        client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Simulate factory machine telemetry over a local IoT Hub emulator.")
    parser.add_argument("--machines", type=int, default=6, help="number of virtual machines (devices)")
    parser.add_argument("--duration", type=float, default=90, help="seconds to run (0 = run forever, Ctrl+C to stop)")
    parser.add_argument("--interval", type=float, default=1.5, help="avg seconds between readings per machine")
    args = parser.parse_args()

    registry = DeviceRegistry()
    threads = []
    stop_event = threading.Event()

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
