import asyncio
import boto3

asyncio.set_event_loop(asyncio.new_event_loop())

client = boto3.client("braket", region_name="eu-north-1")

for status in ["QUEUED", "RUNNING"]:
    response = client.search_quantum_tasks(
        filters=[
            {"name": "deviceArn", "operator": "EQUAL",
             "values": ["arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet"]},
            {"name": "status", "operator": "EQUAL", "values": [status]},
        ],
        maxResults=100
    )
    tasks = response["quantumTasks"]
    print(f"{status}: {len(tasks)} tasks")
    for t in tasks:
        arn = t["quantumTaskArn"]
        try:
            client.cancel_quantum_task(quantumTaskArn=arn)
            print(f"  cancelled {arn.split('/')[-1]}")
        except Exception as e:
            print(f"  could not cancel {arn.split('/')[-1]}: {e}")

print("Done.")
