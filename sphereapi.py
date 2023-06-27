import requests

class SphereApi:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def _call_api(self, path):
        uri = f"{self.base_url}/{path}"
        headers = {'x-api-key': self.api_key}

        response = requests.get(uri, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(response.json())
            raise Exception(f'Failed to fetch api {path}.')

    def data(self, start, end):
        path = f"data?start={start}&end={end}"
        return self._call_api(path)

    def schema(self):
        path = "schema"
        return self._call_api(path)

    def adsize(self, type):
        path = f"adsize?type={type}"
        return self._call_api(path)
