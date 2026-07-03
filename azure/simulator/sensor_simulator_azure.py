"""
sensor_simulator_azure.py

Real Azure IoT Hub version (uses azure-iot-device). This is the exact design from
the build guide - point DEVICE_CONNECTION_STRINGS at your IoT Hub devices and run.
For a version that runs with no Azure account, see local_sim/simulator/sensor_simulator.py.
"""
import time
import json
import random
import uuid
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message

# One connection string per simulated device (register 5-10 devices in IoT Hub)
DEVICE_CONNECTION_STRINGS = [
    "HostName=<your-hub>.azure-devices.net;DeviceId=machine-01;SharedAccessKey=<key1>",
    "HostName=<your-hub>.azure-devices.net;DeviceId=machine-02;SharedAccessKey=<key2>",
    # add more machines...
]

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
        # simulate a malformed/missing-field event to test ADF schema validation later
        del reading["vibration"]

    return reading

def run_device(connection_string: str, machine_id: str):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    client.connect()
    print(f"[{machine_id}] connected")

    tick = 0
    try:
        while True:
            tick += 1
            anomaly = random.random() < 0.03       # ~3% chance of anomaly
            bad_schema = random.random() < 0.01     # ~1% chance of malformed event

            reading = generate_reading(machine_id, inject_anomaly=anomaly, inject_bad_schema=bad_schema)
            msg = Message(json.dumps(reading))
            msg.content_encoding = "utf-8"
            msg.content_type = "application/json"
            client.send_message(msg)
            print(f"[{machine_id}] sent: {reading}")

            time.sleep(random.uniform(2, 5))
    except KeyboardInterrupt:
        client.disconnect()

if __name__ == "__main__":
    import threading
    threads = []
    for i, conn_str in enumerate(DEVICE_CONNECTION_STRINGS, start=1):
        machine_id = f"machine-{i:02d}"
        t = threading.Thread(target=run_device, args=(conn_str, machine_id), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
