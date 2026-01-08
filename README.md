# simple-index-db

A Python tool for managing a local database of PyPI package metadata, including detailed information about packages, versions, files, and wheel tags. It synchronizes with the PyPI Simple API and provides querying capabilities for analyzing package distributions.

## Features

- **Sync with PyPI**: Fetch and store metadata for all PyPI packages using the Simple API
- **Incremental updates**: Only fetch changed packages based on serial numbers
- **Wheel tag analysis**: Parse and store wheel compatibility tags (Python, ABI, platform)
- **Free-threaded Python support**: Query packages with free-threaded Python support (cp313t, cp314t)
- **Conda-forge integration**: Map between conda-forge and PyPI package names
- **SQLite storage**: Efficient local database for querying package metadata

## Installation

This project uses [Pixi](https://pixi.sh/) for dependency management:

```bash
pixi install
```

Alternatively, install with pip:

```bash
pip install -e .
```

## Usage

### Update Database

Synchronize your local database with PyPI:

```bash
simple-index-db update-db
```

This command:
- Fetches the list of all PyPI projects
- Identifies new and updated packages
- Downloads metadata for changed packages
- Updates the local SQLite database (`simple_index_db.sqlite3`)

The update process runs with 4 concurrent workers for efficient fetching.

### Find Free-threaded Python Packages

List conda-forge packages where all PyPI dependencies support free-threaded Python:

```bash
simple-index-db show-free-threaded
```

This queries for packages with wheel files containing `cp313t`, `cp314t`, or `cp314td` ABI tags and checks if all corresponding conda-forge packages have free-threaded support.

## Database Schema

The database stores:

- **Projects**: PyPI project names, status, and serial numbers
- **Versions**: Version strings with VSS validation
- **Files**: Download URLs, hashes, metadata, upload times
- **Wheels**: Parsed wheel compatibility tags
  - Python tags (e.g., `cp313`, `py3`)
  - ABI tags (e.g., `cp313`, `abi3`, `cp313t` for free-threaded)
  - Platform tags (e.g., `linux_x86_64`, `win_amd64`)
  - Build tags (optional build numbers)
- **Hashes**: File integrity hashes (MD5, SHA256, etc.)

## Project Structure

```
src/simple_index_db/
├── main.py         # CLI commands and update logic
├── db.py           # SQLAlchemy models and database schema
├── pypi_client.py  # PyPI Simple API client
└── conda.py        # Conda-forge to PyPI mapping utilities
```

## Requirements

- Python >= 3.11
- SQLAlchemy >= 2.0.15
- Typer (CLI framework)
- Requests (HTTP client)
- msgspec (JSON parsing)
- packaging (version parsing)

## Development

The project includes development tools:

- `scalene` and `py-spy` for profiling
- `ipython` for interactive exploration
- Type stubs for requests

## License

Copyright (c) Klaus Zimmermann
