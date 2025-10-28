from functools import lru_cache

import requests

MAPPING_URL = "https://raw.githubusercontent.com/prefix-dev/parselmouth/main/files/v0/conda-forge/compressed_mapping.json"


@lru_cache(maxsize=None)
def _load_mapping():
    r = requests.get(MAPPING_URL)
    r.raise_for_status()
    return r.json()


@lru_cache(maxsize=None)
def _load_reverse_mapping():
    mapping = _load_mapping()
    reverse_mapping = {}
    for conda_name, pypi_names in mapping.items():
        if pypi_names is None:
            continue
        for pypi_name in pypi_names:
            reverse_mapping.setdefault(pypi_name, []).append(conda_name)
    return reverse_mapping


def get_conda_packages():
    """
    Get the list of all conda package names that have a corresponding PyPI package.

    :return: Conda package names together with names of PyPI packages they depend on.
    """
    mapping = _load_mapping().copy()
    return mapping


def get_pypi_packages():
    """
    Get the list of all PyPI package names that have a corresponding conda package.

    :return: PyPI package names together with names of conda package that depend on them.
    """
    reverse_mapping = _load_reverse_mapping().copy()
    return reverse_mapping


def conda_to_pypi(conda_pkg_name: str) -> str | None:
    """
    Map a conda package name to a PyPI package name using the parselmouth mapping.

    :param conda_pkg_name: The conda package name.
    :return: The corresponding PyPI package name, or None if not found.
    """
    mapping = _load_mapping()
    return mapping.get(conda_pkg_name, [])


def pypi_to_conda(pypi_pkg_name: str) -> list[str]:
    """
    Map a PyPI package name to conda package names using the parselmouth mapping.

    :param pypi_pkg_name: The PyPI package name.
    :return: A list of corresponding conda package names, or an empty list if not found.
    """
    reverse_mapping = _load_reverse_mapping()
    return reverse_mapping.get(pypi_pkg_name, [])