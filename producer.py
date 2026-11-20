
print("Kinesis Producer Started!")

import boto3
import pandas as pd
import json

stream_name = "TripEventsStream"

kinesis = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

df = pd.read_csv("Activity09_Kinesis_trip_events.csv")

print("Total records:", len(df))

for index, row in df.iterrows():

    record = row.to_dict()

    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey=str(record["driver_id"])
    )

    print("Record Sent:", record["trip_id"])
    print("Partition Key:", record["driver_id"])
    print("Sequence Number:", response["SequenceNumber"])

print("All records sent successfully!")
