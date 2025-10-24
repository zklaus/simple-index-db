import re
import time
from queue import Empty, Queue
from threading import Thread

import typer
from requests.exceptions import HTTPError
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from .db import Project, init_db
from .pypi_client import PyPIClient

app = typer.Typer()


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def get_project_list():
    client = PyPIClient()
    return client.get_project_list()


def get_project_info(input_queue, output_queue):
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
    repo_last_serial, projects = get_project_list()
    projects_to_update = Queue()
    num_projects = len(projects)
    num_projects_to_update = 0
    with Session() as session:
        for project_info in projects: #[:10000]:
            new_project_last_serial = project_info["_last-serial"]
            project_name = normalize(project_info["name"])
            try:
                project = session.execute(
                    select(Project).filter_by(name=project_name)
                ).scalar_one()
                old_project_last_serial = project.last_serial
                if project.last_serial >= new_project_last_serial:
                    continue
            except NoResultFound:
                old_project_last_serial = "unknown"
                pass
            # print(f"Queuing project {project_name} for update w/ serial {new_project_last_serial} ({old_project_last_serial})")
            projects_to_update.put(project_name)
            num_projects_to_update += 1
    return num_projects, num_projects_to_update, projects_to_update


@app.command()
def main():
    Session = init_db()
    num_projects, numprojects_to_update, projects_to_update = find_projects_to_update(
        Session
    )
    print(f"Total projects on PyPI: {num_projects}")
    print(f"Projects to update: {numprojects_to_update}")
    project_info_queue = Queue()
    for _ in range(4):
        Thread(
            target=get_project_info,
            args=(projects_to_update, project_info_queue),
        ).start()
    start = time.time()
    with Session() as session:
        num_updated_projects = 0
        while True:
            try:
                project_last_serial, project_info = project_info_queue.get(timeout=1)
                project = session.execute(
                    select(Project).filter_by(name=project_info["name"])
                ).scalar_one_or_none()
                if project is not None:
                #     project.update_from_info(session, project_last_serial, project_info)
                # else:
                    session.delete(project)
                    session.commit()
                project = Project.from_info(
                    session, project_last_serial, project_info
                )
                session.add(project)
                project_info_queue.task_done()
                num_updated_projects += 1
            except Empty:
                if projects_to_update.empty():
                    break
            if num_updated_projects % 100 == 0:
                session.commit()
                end = time.time()
                elapsed = end - start
                percent = num_updated_projects / numprojects_to_update * 100.
                print(
                    f"Updated {num_updated_projects}/{numprojects_to_update} projects ({percent}%) "
                    f"in {elapsed:.2f} seconds (ETT: {elapsed / percent * 100.:.2f} seconds)"
                )
        session.commit()
        print(f"Committed {num_updated_projects} updated projects")
