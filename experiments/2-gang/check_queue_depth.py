import boto3
from braket.aws import AwsDevice, AwsQuantumTask

# Device queue depth
device = AwsDevice("arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet")
device = AwsDevice("arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q")
depth = device.queue_depth()
print(f"IQM Garnet queue depth:")
print(f"  normal tasks : {depth.quantum_tasks}")
print(f"  hybrid jobs  : {depth.jobs}")

# Find your queued tasks
client = boto3.client("braket", region_name="eu-north-1")
response = client.search_quantum_tasks(
    filters=[
        {"name": "deviceArn", "operator": "EQUAL",
         "values": ["arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet"]},
        {"name": "status", "operator": "EQUAL", "values": ["QUEUED"]},
    ],
    maxResults=50
)
print(f"\nYour queued tasks: {len(response['quantumTasks'])}")
for t in response["quantumTasks"]:
    task = AwsQuantumTask(arn=t["quantumTaskArn"])
    pos  = task.queue_position()
    print(f"  {t['quantumTaskArn'].split('/')[-1]}  position={pos.queue_position}")

