import json
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1

PROJECT_ID = "chunky-dataeng"
SUBSCRIPTION_ID = "analysis_sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


def utc_now_string():
    return datetime.now(timezone.utc).isoformat()


def parse_breadcrumb_timestamp(record):
    opd_date = record.get("OPD_DATE")
    act_time = record.get("ACT_TIME")

    if opd_date is None or act_time is None:
        return None

    try:
        base_date = datetime.strptime(str(opd_date), "%d%b%Y:%H:%M:%S")
        return base_date.timestamp() + int(act_time)
    except Exception:
        return None


class DailyAnalysis:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first_receive_time = None
        self.first_receive_timestamp_str = None
        self.unique_vehicle_ids = set()
        self.unique_trip_ids = set()
        self.total_breadcrumbs = 0
        self.min_breadcrumb_timestamp = None
        self.max_breadcrumb_timestamp = None
        self.expected_breadcrumbs = None
        self.sentinel_received_time_str = None

    def process_breadcrumb(self, record):
        if self.first_receive_time is None:
            self.first_receive_time = time.time()
            self.first_receive_timestamp_str = utc_now_string()

        self.total_breadcrumbs += 1

        vehicle_id = record.get("vehicle_id")
        if vehicle_id is not None:
            self.unique_vehicle_ids.add(vehicle_id)

        trip_id = record.get("EVENT_NO_TRIP")
        if trip_id is not None:
            self.unique_trip_ids.add(trip_id)

        bc_ts = parse_breadcrumb_timestamp(record)
        if bc_ts is not None:
            if self.min_breadcrumb_timestamp is None or bc_ts < self.min_breadcrumb_timestamp:
                self.min_breadcrumb_timestamp = bc_ts
            if self.max_breadcrumb_timestamp is None or bc_ts > self.max_breadcrumb_timestamp:
                self.max_breadcrumb_timestamp = bc_ts

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

        elapsed = time.time() - self.first_receive_time
        throughput = self.total_breadcrumbs / elapsed if elapsed > 0 else 0.0

        min_ts_str = (
            datetime.fromtimestamp(self.min_breadcrumb_timestamp, tz=timezone.utc).isoformat()
            if self.min_breadcrumb_timestamp is not None else "N/A"
        )
        max_ts_str = (
            datetime.fromtimestamp(self.max_breadcrumb_timestamp, tz=timezone.utc).isoformat()
            if self.max_breadcrumb_timestamp is not None else "N/A"
        )

        print(f"BEGIN_TIMESTAMP: {self.first_receive_timestamp_str}")
        print(f"NUM_VEHICLES: {len(self.unique_vehicle_ids)}")
        print(f"MIN_BC_TIMESTAMP: {min_ts_str}")
        print(f"MAX_BC_TIMESTAMP: {max_ts_str}")
        print(f"NUM_TRIPS: {len(self.unique_trip_ids)}")
        print(f"NUM_BREADCRUMBS: {self.total_breadcrumbs}")
        print(f"END_TIMESTAMP: {self.sentinel_received_time_str}")
        print(f"WALLTIME: {elapsed:.3f}")
        print(f"THROUGHPUT: {throughput:.3f}")


def main():
    print(f"Listening on {subscription_path}")
    daily = DailyAnalysis()

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