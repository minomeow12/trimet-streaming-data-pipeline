import gzip
import json
import os
import shutil
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1

PROJECT_ID = "chunky-dataeng"
SUBSCRIPTION_ID = "backup_sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


def utc_now_string():
    return datetime.now(timezone.utc).isoformat()


def local_date_string():
    return datetime.now().strftime("%Y-%m-%d")


class DailyBackup:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first_receive_time = None
        self.first_receive_timestamp_str = None
        self.total_breadcrumbs = 0
        self.unique_vehicle_ids = set()
        self.current_filename = None
        self.current_file = None
        self.expected_breadcrumbs = None
        self.sentinel_received_time_str = None

    def ensure_file_open(self):
        if self.current_file is None:
            self.current_filename = f"breadcrumbs_{local_date_string()}.log"
            self.current_file = open(self.current_filename, "w", encoding="utf-8")

    def process_breadcrumb(self, record):
        if self.first_receive_time is None:
            self.first_receive_time = time.time()
            self.first_receive_timestamp_str = utc_now_string()

        self.ensure_file_open()
        self.current_file.write(json.dumps(record) + "\n")

        self.total_breadcrumbs += 1

        vehicle_id = record.get("vehicle_id")
        if vehicle_id is not None:
            self.unique_vehicle_ids.add(vehicle_id)

    def process_sentinel(self, record):
        self.expected_breadcrumbs = int(record.get("expected_breadcrumbs", 0))
        self.sentinel_received_time_str = utc_now_string()

    def ready_to_finish(self):
        return (
            self.expected_breadcrumbs is not None
            and self.sentinel_received_time_str is not None
            and self.total_breadcrumbs >= self.expected_breadcrumbs
        )

    def log_summary(self):
        if self.first_receive_time is None:
            print("Received sentinel but no breadcrumbs were processed.")
            return

        if self.current_file is not None:
            self.current_file.close()

        data_size = os.path.getsize(self.current_filename) if self.current_filename else 0
        compressed_filename = f"{self.current_filename}.gz"

        with open(self.current_filename, "rb") as src:
            with gzip.open(compressed_filename, "wb") as dst:
                shutil.copyfileobj(src, dst)

        elapsed = time.time() - self.first_receive_time
        throughput = self.total_breadcrumbs / elapsed if elapsed > 0 else 0.0

        print(f"BEGIN_TIMESTAMP: {self.first_receive_timestamp_str}")
        print(f"NUM_BREADCRUMBS: {self.total_breadcrumbs}")
        print(f"DATASIZE: {data_size}")
        print(f"NUM_VEHICLES: {len(self.unique_vehicle_ids)}")
        print(f"END_TIMESTAMP: {self.sentinel_received_time_str}")
        print(f"WALLTIME: {elapsed:.3f}")
        print(f"THROUGHPUT: {throughput:.3f}")


def main():
    print(f"Listening on {subscription_path}")
    daily = DailyBackup()

    while True:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 1000,
            },
            timeout=30,
        )

        ack_ids = []

        for received_message in response.received_messages:
            ack_ids.append(received_message.ack_id)

            try:
                payload = json.loads(received_message.message.data.decode("utf-8"))
            except Exception:
                continue

            if payload.get("message_type") == "sentinel":
                daily.process_sentinel(payload)
            else:
                daily.process_breadcrumb(payload)

        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )

        if daily.ready_to_finish():
            daily.log_summary()
            daily.reset()


if __name__ == "__main__":
    main()