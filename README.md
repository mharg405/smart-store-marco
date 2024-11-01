# smart-store-marco
P1. Project Start &amp; Planning (GitHub Repo, clone down, organize)
Project Setup Instructions
**1. Create Local Project Virtual Environment (One-Time Task)**  
Use only python3 commands; py doesn't work right.  
```bash
python3 -m venv .venv

This command creates a new virtual environment in a folder named .venv. A virtual environment is an isolated workspace where you can install packages without affecting your system Python installation.

2. Activate Virtual Environment (Every Time we Open a Terminal to work on the Project)
.venv\Scripts\activate
This command activates the virtual environment. After activation, any packages you install will only affect this environment.

3. Install Requirements
python3 -m pip install --upgrade -r requirements.txt
This command installs all the packages listed in the requirements.txt file, ensuring they are up to date. It uses pip, the package installer for Python.

4. Create utils/logger.py
In VS Code, use File / New Folder to create a new folder named utils to hold your utility scripts. I will provide these. You are encouraged to use them exactly as provided. In this folder, create a file named logger.py (exactly). Copy and paste the content from the starter repo linked above.
Folder were already created

5. Create scripts/data_prep.py
In VS Code, use File / New Folder to create a new folder named scripts to hold your scripts. In this folder, create a file named data_prep.py (exactly). Copy and paste the content from the starter repo linked above.
Folder were already created

6. Run Python Script
python3 scripts/data_prep.py
This command runs the Python script located in the scripts folder named data_prep.py. It executes the code written in that file.

7. Git add-commit-push Your Work Back up to GitHub Project Repository
git add .
This command stages all changes in your project directory for the next commit.

git commit -m "add starter files"
This command creates a new commit with a message describing the changes you've made. Here, it's noting that you've added starter files.

git push -u origin main
This command pushes your committed changes to the main branch of your remote GitHub repository, making your changes available online.