import requests

# Sphere configuration
API_KEY = 'L6uHUmF1934CBenPeLZJQ4jlnPUbWxIk5X286aDv'  # replace with your API key

class SphereApi:
    def __init__(self,
        api_key = API_KEY):
        
        self.base_url = "https://api.getsphere.ai/blocklogs"
        self.api_key = api_key

    def _call_api(self, method, path, body = None):
        uri = f"{self.base_url}/{path}"
        headers = {'x-api-key': self.api_key}

        response = requests.request(
            method=method,
            url=uri,
            headers=headers,
            json=body)

        if response.status_code >= 200 and response.status_code < 400:
            return response.json()
        else:
            print(response.json())
            raise Exception(f'Failed to fetch api {path}.')

    def data(self, start, end):
        path = f"data?start={start}&end={end}"
        return self._call_api(
            method='GET',
            path=path)

    def schema(self):
        path = "schema"
        return self._call_api('GET',path)

    def schema_v2(self):
        path = "schema/v2"
        return self._call_api('GET',path)

    def adsize(self, type):
        path = f"adsize?type={type}"
        return self._call_api(
            method='GET',
            path=path)

    def query_start(self, start, end):
        path = f"query/start"
        body = {
            "start_epoch_milliseconds": start,
            "end_epoch_milliseconds": end
        }
        return self._call_api(
            method='POST', 
            path=path,
            body=body)

    def query_start_async(self, start, end):
        path = f"query/startasync"
        body = {
            "start_epoch_milliseconds": start,
            "end_epoch_milliseconds": end
        }
        return self._call_api(
            method='POST', 
            path=path,
            body=body)

    def query_status(self, query_execution_id):
        path = f"query/{query_execution_id}/status"
        return self._call_api(
            method='GET', 
            path=path)

    def query_stop(self, query_execution_id):
        path = f"query/{query_execution_id}/stop"
        return self._call_api(
            method='POST', 
            path=path)

    def query_data(self, query_execution_id, next_token, max_rows = None):
        path = f"query/{query_execution_id}/data"
        body = {
            "next_token": next_token
        }

        if max_rows:
            body["max_rows"] = max_rows

        return self._call_api(
            method='GET', 
            path=path, 
            body=body)