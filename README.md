# smart-store-marco
P1. Project Start &amp; Planning (GitHub Repo, clone down, organize)

Project Setup Instructions

**1. Create Local Project Virtual Environment (One-Time Task)**  
Use only `python3` commands; `py` doesn't work right. cd to "C:\Users\4harg\Documents\smart-store-marco" 
```bash
python3 -m venv .venv`
```
> This command creates a new virtual environment in a folder named `.venv`. A virtual environment is an isolated workspace where you can install packages without affecting your system Python installation.


**2. Activate Virtual Environment (Every Time we Open a Terminal to work on the Project)**  

>First use:

```bash
Set-ExecutionPolicy RemoteSigned -Scope Process
```
Then:

```bash
.venv\Scripts\activate
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
python3 scripts/data_prep.py
```
> This command runs the Python script located in the `scripts` folder, executing any code contained within.


**7. Git add-commit-push Your Work Back up to GitHub Project Repository**  
```bash
git add .
```
> This command stages all your changes for the next commit. 
 
```bash
git commit -m "add starter files"
```
> This command creates a new commit with a message describing what you’ve done.  

```bash
git push -u origin main
```
> This command uploads your committed changes to the `main` branch of your GitHub repository.

### P3. Prepare Data for ETL
Error: 
> FAIL: test_format_column_strings_to_upper_and_trim (__main__.TestDataScrubber.test_format_column_strings_to_upper_and_trim)     
----------------------------------------------------------------------
Traceback (most recent call last):
  File "C:\Users\4harg\Documents\smart-store-marco\test\test_data_scrubber.py", line 87, in test_format_column_strings_to_upper_and_trim
    self.assertTrue(df_formatted['Name'].str.isupper().all(), "Strings not formatted to uppercase correctly")
AssertionError: False is not true : Strings not formatted to uppercase correctly

Fix:
```python
def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        """
        Format strings in a specified column by converting to uppercase and trimming whitespace.
    
        Parameters:
            column (str): Name of the column to format.
    
        Returns:
            pd.DataFrame: Updated DataFrame with formatted string column.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            # Apply uppercasing and trimming
            self.df[column] = (
                self.df[column]
                .fillna("")    # Handle NaNs by filling with an empty string
                .astype(str)   # Convert all entries to string type
                .str.strip()   # Remove leading and trailing whitespace
                .str.upper()   # Convert strings to uppercase
            )
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

```

- **Added functionally to save prepared files**
  > Saved prepared files to prepared directory in data_prep.py script.

Utils modelue not found error popped up and the fix was to add:

```python
# Now we can import local modules
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402
```
after PROJECT_ROOT

# Smart Sales


## Schema
![Smart Sales Database (Schema)](images/schema.png)

### Customers Table
![Smart Sales Database (Customers)](images/customersTable.jpg)

### Product Table
![Smart Sales Database (Products)](images/productsTable.jpg)

### Sales Table
![Smart Sales Database (Sales)](images/salesTable.jpg)