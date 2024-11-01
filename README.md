# smart-store-marco
P1. Project Start &amp; Planning (GitHub Repo, clone down, organize)

Project Setup Instructions

**1. Create Local Project Virtual Environment (One-Time Task)**  
Use only `python3` commands; `py` doesn't work right.  
```bash
python3 -m venv .venv`
```
> This command creates a new virtual environment in a folder named `.venv`. A virtual environment is an isolated workspace where you can install packages without affecting your system Python installation.

**2. Activate Virtual Environment (Every Time we Open a Terminal to work on the Project)**  
```bash
`.venv\Scripts\activate`
```
> This command activates the virtual environment, allowing you to use the installed packages in your project.

**3. Install Requirements**  
```bash
`python3 -m pip install --upgrade -r requirements.txt`
```
> This command installs the required packages listed in `requirements.txt`, ensuring your project has all the dependencies it needs.

**4. Create utils/logger.py**  

In VS Code, use File / New Folder to create a new folder named `utils` to hold your utility scripts. I will provide these. You are encouraged to use them exactly as provided. In this folder, create a file named `logger.py` (exactly). Copy and paste the content from the starter repo linked above.

**5. Create scripts/data_prep.py**  

In VS Code, use File / New Folder to create a new folder named `scripts` to hold your scripts. In this folder, create a file named `data_prep.py` (exactly). Copy and paste the content from the starter repo linked above.

**6. Run Python Script**  
```bash
`python3 scripts/data_prep.py`
```
> This command runs the Python script located in the `scripts` folder, executing any code contained within.

**7. Git add-commit-push Your Work Back up to GitHub Project Repository**  
```bash
`git add .`
```
> This command stages all your changes for the next commit.  
```bash
`git commit -m "add starter files"`
```
> This command creates a new commit with a message describing what you’ve done.  
```bash
`git push -u origin main`
```
> This command uploads your committed changes to the `main` branch of your GitHub repository.
