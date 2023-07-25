from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB Configuration
INFLUXDB_URL = "http://localhost:8086"  # replace with your InfluxDB URL
INFLUXDB_TOKEN = "your_token"  # replace with your InfluxDB token
INFLUXDB_ORG = "your_org"  # replace with your InfluxDB org

INFLUXDB_BUCKET = None  # replace with your InfluxDB bucket

class InfluxExport:
    def __init__(self,
        influxdb_url = INFLUXDB_URL,
        influxdb_token = INFLUXDB_TOKEN,
        influxdb_org = INFLUXDB_ORG):
        
        self.influxdb_url = influxdb_url
        self.influxdb_token = influxdb_token
        self.influxdb_org = influxdb_org

    def export(self, data, schema, bucket = INFLUXDB_BUCKET):
        if not bucket:
            return

        client = InfluxDBClient(
            url=self.influxdb_url,
            token=self.influxdb_token)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        sequence = []
        tags = [col['name'] for col in schema['table']['columns'] if col['type'] == "string"]
        fields = [col['name'] for col in schema['table']['columns'] if col['type'] == "int64"]

        for _, row in data.iterrows():
            point = Point("blocklogs")
            for tag in tags:
                point = point.tag(tag, row[tag])
            for field in fields:
                point = point.field(field, row[field])
            point = point.time(row["epoch_in_usecs"], WritePrecision.S)
            sequence.append(point)

        write_api.write(
            bucket=bucket,
            org=self.influxdb_org,
            record=sequence)
