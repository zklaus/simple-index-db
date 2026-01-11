import re
import time
from importlib.metadata import version
from queue import Empty, Queue
from threading import Thread

import typer
from requests.exceptions import HTTPError
from rich.console import Console
from sqlalchemy import select

from .conda import (
    get_conda_packages,
    get_pypi_packages,
)
from .db import (
    AbiTag,
    File,
    Project,
    Wheel,
    init_db,
)
from .pypi_client import PyPIClient

app = typer.Typer()

error_console = Console(stderr=True)


def normalize(name):
    """
    Normalize a project name according to the name normalization specification.

    This specification was preceded by and is essentially identical to PEP 503.
    https://packaging.python.org/en/latest/specifications/name-normalization/#name-normalization

    :param name: The project name to normalize.
    :return: The normalized project name.
    """
    return re.sub(r"[-_.]+", "-", name).lower()


def get_project_list():
    """
    Get the list of all projects on PyPI.

    :return: A tuple of (repo_last_serial, projects), where repo_last_serial is the
             last serial number of the PyPI repository, and projects is a list of
             project info dictionaries, containing at least 'name' and '_last-serial' keys,
             where the name is *not* normalized.
    """
    client = PyPIClient()
    return client.get_project_list()


def get_project_info(input_queue, output_queue):
    """
    Worker function to get project info from PyPI.

    Reads project names from the input_queue, fetches their info from PyPI,
    and puts the project info dictionaries into the output_queue.
    """
    client = PyPIClient()
    while True:
        try:
            project_name = input_queue.get(block=False)
        except Empty:
            break
        try:
            project = client.get_project(project_name)
            output_queue.put(project)
        except HTTPError:
            print(f"Failed to fetch project {project_name}")
        input_queue.task_done()


def find_projects_to_update(Session):
    """
    Find projects that need to be updated or added in the local database.

    Compares the local database with the PyPI project list to determine which projects
    need to be updated (i.e., their last serial number has increased) or added (i.e., they are new projects).

    :param Session: The SQLAlchemy session factory.
    :return: A tuple containing:
             - num_projects: Total number of projects on PyPI.
             - num_projects_to_update: Number of projects that need to be updated.
             - projects_to_update: Queue of project names to update.
             - num_projects_to_add: Number of new projects to add.
             - projects_to_add: Queue of project names to add.
    """
    repo_last_serial, projects = get_project_list()
    new_projects = {
        normalize(project_info["name"]): project_info["_last-serial"]
        for project_info in projects
    }
    projects_to_update = Queue()
    num_projects = len(projects)
    num_projects_to_update = 0
    with Session() as session:
        for name, old_last_serial in session.execute(
            select(Project.name, Project.last_serial)
        ):
            try:
                new_last_serial = new_projects.pop(name)
            except KeyError:
                continue
            if old_last_serial < new_last_serial:
                projects_to_update.put(name)
                num_projects_to_update += 1
    projects_to_add = Queue()
    num_projects_to_add = 0
    for name in new_projects.keys():
        projects_to_add.put(name)
        num_projects_to_add += 1
    return (
        num_projects,
        num_projects_to_update,
        projects_to_update,
        num_projects_to_add,
        projects_to_add,
    )


def process_updates(Session, project_queue, num_projects, update=False):
    project_info_queue = Queue()
    for _ in range(4):
        Thread(
            target=get_project_info,
            args=(project_queue, project_info_queue),
        ).start()
    start = time.time()
    num_updated_projects = 0
    with Session() as session:
        while True:
            try:
                project_last_serial, project_info = project_info_queue.get(timeout=1)
                if update:
                    project = session.execute(
                        select(Project).filter_by(name=project_info["name"])
                    ).scalar_one_or_none()
                else:
                    project = None
                if project is None:
                    project = Project.from_info(
                        session, project_last_serial, project_info
                    )
                    session.add(project)
                else:
                    project.update_from_info(session, project_last_serial, project_info)
                project_info_queue.task_done()
                num_updated_projects += 1
            except Empty:
                if project_queue.empty():
                    break
            if num_updated_projects % 100 == 0:
                session.commit()
                end = time.time()
                elapsed = end - start
                percent = num_updated_projects / num_projects * 100.0
                error_console.print(
                    f"Updated {num_updated_projects}/{num_projects} projects ({percent:.2f}%) "
                    f"in {elapsed:.2f} seconds (ETT: {elapsed / percent * 100.0:.2f} seconds)"
                )
        session.commit()
    error_console.print(f"Committed {num_updated_projects} updated projects")


@app.command()
def update_db():
    Session = init_db(error_console)
    (
        num_projects,
        num_projects_to_update,
        projects_to_update,
        num_projects_to_add,
        projects_to_add,
    ) = find_projects_to_update(Session)
    error_console.print(f"Total projects on PyPI: {num_projects}")
    error_console.print(f"Projects to update: {num_projects_to_update}")
    error_console.print(f"Projects to add: {num_projects_to_add}")

    process_updates(Session, projects_to_update, num_projects_to_update, update=True)
    process_updates(Session, projects_to_add, num_projects_to_add, update=False)


def _setup_output_console():
    console = Console()
    return console


def _find_ready_packages(session):
    pypi_packages = list(get_pypi_packages().keys())
    stmt = (
        select(Project.name)
        .distinct()
        .join(Project.files)
        .join(File.wheel)
        .join(Wheel.abi_tag)
        .filter(
            Project.name.in_(pypi_packages)
            & ((AbiTag.tag.like("%cp314t")) | (AbiTag.tag.like("%cp314td")))
        )
    )
    pkgs = session.scalars(stmt).all()
    ready_packages = []
    for conda_pkg, pypi_pkgs in get_conda_packages().items():
        if pypi_pkgs is None:
            continue
        if all([pypi_pkg in pkgs for pypi_pkg in pypi_pkgs]):
            ready_packages.append(conda_pkg)
    return ready_packages


def _get_header_info(session, ready_packages):
    header_info = {
        "version": version("simple-index-db"),
        "ts": int(time.time()),
        "ready_packages": len(ready_packages),
    }
    return header_info


def _print_header(console, header_info):
    console.print("# This file was created with simple-index-db v0.1.0.")
    console.print("# It is based on data from PyPI's simple index as follows:")
    console.print(f"# simple-index-db version: {header_info['version']}")
    console.print(f"# ts: {header_info['ts']}")
    console.print(
        f"# date: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(header_info['ts']))}"
    )
    console.print("# Last serial of package listing: xxx")
    console.print("# Last serial of received package data:")
    console.print("# Stats: xxx")
    console.print(f"# Ready Packages: {header_info['ready_packages']}")


def _print_packages(console, ready_packages):
    for pkg in sorted(ready_packages):
        console.print(f"{pkg}")


@app.command()
def show_free_threaded():
    console = _setup_output_console()
    Session = init_db(error_console)
    with Session() as session:
        ready_packages = _find_ready_packages(session)
        header_info = _get_header_info(session, ready_packages)
    _print_header(console, header_info)
    _print_packages(console, ready_packages)
