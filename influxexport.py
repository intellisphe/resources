from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxExport:
    def __init__(self, influxdb_url, influxdb_token, influxdb_org, influxdb_bucket):
        self.influxdb_url = influxdb_url
        self.influxdb_token = influxdb_token
        self.influxdb_org = influxdb_org
        self.influxdb_bucket = influxdb_bucket

    def export(self, data, schema):
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
            bucket=self.influxdb_bucket,
            org=self.influxdb_org,
            record=sequence)
