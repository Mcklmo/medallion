import importlib.resources as resources
import os
from pathlib import Path
import shutil
import tempfile

from medallion.configure_entrypoint.generate_docker_compose import (
    generate_docker_compose,
)
from medallion.configure_entrypoint.vscode import (
    DEFAULT_CONFIG_YAML_FILE_NAME,
    load_config,
    write_launch_json_file,
)
from medallion.store.base import load_dotenv_from_medallion_root


def start_project(user_project_name: str):
    write_file_tree(user_project_name)
    load_dotenv_from_medallion_root()

    graph = load_config(DEFAULT_CONFIG_YAML_FILE_NAME)
    write_launch_json_file()
    generate_docker_compose(
        DEFAULT_CONFIG_YAML_FILE_NAME.name,
        Path("docker-compose.yml"),
        graph,
    )


def write_file_tree(user_project_name):
    template_root = resources.files("medallion").joinpath(
        "example",
        "blank_project",
    )
    project_name_base = "my-new-project"

    # copy files to /tmp/ folder

    temp_dir = tempfile.mkdtemp()
    with resources.as_file(template_root) as template_files_path:
        for item in os.listdir(template_files_path):
            s = os.path.join(template_files_path, item)
            d = os.path.join(temp_dir, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)

    update_file_names = [
        (
            "example.env",
            ".env",
        )
    ]

    # replace project name in files and normalize file names: rename mapped
    # files (e.g. example.env -> .env) and strip the .py-tpl suffix used to
    # keep template sources from being byte-compiled at install time. walking
    # the tree here handles nested files (e.g. src/*.py-tpl), which the
    # top-level copy loop below would otherwise miss.
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "r") as f:
                content = f.read()

            content = content.replace(
                project_name_base,
                user_project_name,
            )

            with open(file_path, "w") as f:
                f.write(content)

            new_name = file
            for old_name, mapped_name in update_file_names:
                if new_name == old_name:
                    new_name = mapped_name
                    break

            if new_name.endswith(".py-tpl"):
                new_name = new_name[: -len("-tpl")]

            if new_name != file:
                os.rename(file_path, os.path.join(root, new_name))

    # write files to current directory
    for item in os.listdir(temp_dir):
        s = os.path.join(temp_dir, item)
        d = os.path.join(os.getcwd(), item)

        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)

    shutil.rmtree(temp_dir)
