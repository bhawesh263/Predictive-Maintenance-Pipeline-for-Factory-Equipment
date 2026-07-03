"""
iot_hub_emulator.py

Local stand-in for Azure IoT Hub's device-to-cloud ingestion.

What it mirrors from the real service:
  - Per-device identity (a device must be "registered" with a fake key before it can send)
  - A durable message queue that downstream consumers read from with a consumer-group
    style checkpoint (so re-running the stream processor doesn't reprocess old messages)
  - Basic message envelope (content type, timestamp) similar to azure.iot.device.Message

What it does NOT try to emulate: retry/backoff, TLS, real device provisioning, throughput
quotas. Those are genuine reasons to use IoT Hub in production - see azure/ for the real
device SDK version of this script.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone

QUEUE_PATH = os.path.join(os.path.dirname(__file__), "..", "storage", "iot_hub_queue.jsonl")
QUEUE_PATH = os.path.abspath(QUEUE_PATH)

_lock = threading.Lock()


class DeviceRegistry:
    """Fake device identity store, analogous to `az iot hub device-identity create`."""

    def __init__(self):
        self._devices = {}

    def register(self, device_id: str) -> str:
        key = f"fake-key-{device_id}"
        self._devices[device_id] = key
        return key

    def is_registered(self, device_id: str) -> bool:
        return device_id in self._devices


class IoTHubClient:
    """Stand-in for azure.iot.device.IoTHubDeviceClient."""

    def __init__(self, device_id: str, registry: DeviceRegistry):
        if not registry.is_registered(device_id):
            raise PermissionError(f"device '{device_id}' is not registered - call registry.register() first")
        self.device_id = device_id
        os.makedirs(os.path.dirname(QUEUE_PATH), exist_ok=True)

    def connect(self):
        return True

    def send_message(self, payload: dict):
        envelope = {
            "device_id": self.device_id,
            "enqueued_time": datetime.now(timezone.utc).isoformat(),
            "content_type": "application/json",
            "body": payload,
        }
        with _lock:
            with open(QUEUE_PATH, "a") as f:
                f.write(json.dumps(envelope) + "\n")

    def disconnect(self):
        return True


def read_new_messages(checkpoint_offset: int):
    """
    Consumer-group style read: return messages appended after `checkpoint_offset`
    (a byte offset into the queue file) plus the new offset to persist.
    """
    if not os.path.exists(QUEUE_PATH):
        return [], checkpoint_offset

    messages = []
    with open(QUEUE_PATH, "r") as f:
        f.seek(checkpoint_offset)
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        new_offset = f.tell()

    return messages, new_offset
