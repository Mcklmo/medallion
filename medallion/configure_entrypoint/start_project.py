import importlib.resources as resources
import os
import shutil
import tempfile


def start_project(user_project_name: str):
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

    # replace project name in files
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

    # write files to current directory
    update_file_names = [
        (
            "example.env",
            ".env",
        )
    ]

    for item in os.listdir(temp_dir):
        s = os.path.join(temp_dir, item)
        d = os.path.join(os.getcwd(), item)

        for old_name, new_name in update_file_names:
            if item == old_name:
                d = os.path.join(os.getcwd(), new_name)
                break

        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)

    shutil.rmtree(temp_dir)
