import json
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1

PROJECT_ID = "chunky-dataeng"
TOPIC_ID = "bc_topic"

VEHICLE_IDS = [
    2901, 2904, 2905, 2906, 2908, 2910, 2911, 2916, 2922, 2929,
    2935, 2937, 2939, 3001, 3014, 3015, 3019, 3022, 3026, 3030,
    3031, 3035, 3038, 3040, 3042, 3046, 3052, 3054, 3055, 3056,
    3057, 3059, 3102, 3105, 3107, 3108, 3110, 3114, 3116, 3122,
    3124, 3125, 3126, 3128, 3132, 3140, 3142, 3146, 3154, 3164,
    3166, 3167, 3170, 3201, 3209, 3215, 3217, 3218, 3219, 3226,
    3229, 3235, 3241, 3242, 3244, 3246, 3255, 3261, 3263, 3267,
    3303, 3305, 3306, 3311, 3312, 3314, 3317, 3320, 3322, 3323,
    3324, 3327, 3328, 3401, 3402, 3405, 3407, 3415, 3420, 3421,
    3506, 3507, 3508, 3509, 3510, 3517, 3530, 3532, 3533, 3539,
    3555, 3562, 3563, 3564, 3565, 3566, 3568, 3571, 3572, 3574,
    3577, 3607, 3609, 3613, 3615, 3616, 3618, 3621, 3623, 3625,
    3629, 3630, 3632, 3637, 3643, 3645, 3646, 3647, 3648, 3649,
    3702, 3704, 3705, 3707, 3711, 3712, 3716, 3717, 3722, 3723,
    3724, 3725, 3727, 3733, 3734, 3735, 3738, 3740, 3742, 3746,
    3754, 3801, 3803, 3903, 3909, 3911, 3915, 3920, 3921, 3922,
    3933, 3936, 3942, 3945, 3947, 3949, 3951, 3952, 3955, 3957,
    3958, 3959, 3960, 3962, 4003, 4006, 4011, 4015, 4018, 4019,
    4025, 4026, 4028, 4029, 4031, 4033, 4035, 4041, 4043, 4045,
    4050, 4052, 4054, 4060, 4062, 4067, 4069, 4202, 4203, 4205,
    4206, 4208, 4209, 4215, 4217, 4219, 4220, 4223, 4228, 4234,
    4235, 4239, 4304, 4508, 4511, 4513, 4515, 4517, 4518, 4527,
    4530, 4531
]

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"

batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=100,
    max_latency=0.05,
)

publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def utc_now_string():
    return datetime.now(timezone.utc).isoformat()


def fetch_breadcrumbs(vehicle_id):
    url = API_URL.format(vehicle_id=vehicle_id)
    try:
        response = requests.get(url, timeout=(3, 8))
        if response.status_code == 404:
            return []
        response.raise_for_status()
        data = response.json()
    except Exception as error:
        print(f"WARNING: failed vehicle_id {vehicle_id}: {error}", flush=True)
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "results" in data:
        return data["results"]
    return []


def main():
    begin_timestamp = utc_now_string()
    start_time = time.time()

    unique_vehicle_ids_with_data = set()
    publish_errors = 0
    futures = []

    for vehicle_id in VEHICLE_IDS:
        breadcrumbs = fetch_breadcrumbs(vehicle_id)

        if breadcrumbs:
            unique_vehicle_ids_with_data.add(vehicle_id)

        for breadcrumb in breadcrumbs:
            breadcrumb["vehicle_id"] = vehicle_id
            message_bytes = json.dumps(breadcrumb).encode("utf-8")
            future = publisher.publish(topic_path, message_bytes)
            futures.append((vehicle_id, future))

    total_breadcrumbs_published = 0

    for vehicle_id, future in futures:
        try:
            future.result()
            total_breadcrumbs_published += 1
        except Exception as error:
            publish_errors += 1
            print(f"WARNING: publish failed for vehicle_id {vehicle_id}: {error}", flush=True)

    sentinel_timestamp = utc_now_string()
    sentinel_payload = {
        "message_type": "sentinel",
        "team": "Sunny",
        "expected_breadcrumbs": total_breadcrumbs_published,
        "sent_at": sentinel_timestamp,
    }

    publisher.publish(
        topic_path,
        json.dumps(sentinel_payload).encode("utf-8")
    ).result()

    elapsed = time.time() - start_time
    throughput = total_breadcrumbs_published / elapsed if elapsed > 0 else 0.0

    print(f"BEGIN_TIMESTAMP: {begin_timestamp}", flush=True)
    print(f"NUM_VEHICLES: {len(unique_vehicle_ids_with_data)}", flush=True)
    print(f"NUM_BREADCRUMBS: {total_breadcrumbs_published}", flush=True)
    print(f"WALLTIME: {elapsed:.3f}", flush=True)
    print(f"THROUGHPUT: {throughput:.3f}", flush=True)
    print(f"END_TIMESTAMP: {sentinel_timestamp}", flush=True)

    if publish_errors > 0:
        print(f"PUBLISH_ERRORS: {publish_errors}", flush=True)


if __name__ == "__main__":
    main()