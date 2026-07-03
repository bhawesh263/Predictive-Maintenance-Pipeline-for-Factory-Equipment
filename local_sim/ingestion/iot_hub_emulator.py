# Local stand-in for Azure IoT Hub device-to-cloud ingestion.
#
# Real IoT Hub gives you per-device identity, retries, TLS, throughput limits, etc.
# This just needs to look enough like it to exercise the rest of the pipeline:
# devices have to register before they can send, and messages land in an
# append-only file that downstream readers can checkpoint against.

import json
import os
import threading
from datetime import datetime, timezone

QUEUE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "storage", "iot_hub_queue.jsonl"))

_lock = threading.Lock()


class DeviceRegistry:
    """Fake device identity store - like `az iot hub device-identity create`, but in memory."""

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
            raise PermissionError(f"device '{device_id}' isn't registered - call registry.register() first")
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
        with _lock, open(QUEUE_PATH, "a") as f:
            f.write(json.dumps(envelope) + "\n")

    def disconnect(self):
        return True


def read_new_messages(checkpoint_offset: int):
    """Return messages appended after checkpoint_offset (a byte offset), plus the new offset."""
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
                # half-written line from a concurrent writer - pick it up next poll
                continue
        new_offset = f.tell()

    return messages, new_offset
