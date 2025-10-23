import requests


class PyPIClient:
    """Client for interacting with the PyPI Simple API."""

    def __init__(self):
        self.base_url = "https://pypi.org/simple/"
        self.session = requests.Session()

    def check_meta(self, meta: dict[str, str]) -> int:
        """Check the metadata of a PyPI response."""
        major, minor = map(int, meta["api-version"].split("."))
        assert major == 1 and minor >= 4, (
            f"Unsupported API version {meta['api-version']}"
        )
        return int(meta["_last-serial"])

    def get_project_list(self) -> tuple[int, list[dict[str, int | str]]]:
        """Fetch the list of all PyPI projects."""
        response = self.session.get(
            self.base_url,
            headers={"Accept": "application/vnd.pypi.simple.v1+json"},
        )
        response.raise_for_status()
        header_last_serial = int(response.headers.get("x-pypi-last-serial"))
        json_body = response.json()
        body_last_serial = self.check_meta(json_body["meta"])
        assert body_last_serial == header_last_serial, (
            "Header last serial does not match body last serial"
        )
        overall_last_serial = body_last_serial
        return (overall_last_serial, json_body["projects"])

    def get_project(self, project_name: str):
        """Fetch details for a specific PyPI project."""
        url = f"{self.base_url}{project_name}/"
        response = self.session.get(
            url,
            headers={"Accept": "application/vnd.pypi.simple.v1+json"},
        )
        response.raise_for_status()
        header_last_serial = int(response.headers.get("x-pypi-last-serial"))
        json_body = response.json()
        body_last_serial = self.check_meta(json_body.pop("meta"))
        assert body_last_serial == header_last_serial, (
            "Header last serial does not match body last serial"
        )
        project_last_serial = body_last_serial
        return (project_last_serial, json_body)
