# Snowflake Table Comparison Tool

This repository helps you compare two Snowflake tables by:

- Comparing the **row counts** of both tables (optionally filtered with a `WHERE` clause)
- Checking for column mismatches (present in one table but not the other)
- Comparing the **distinct values** and **their counts** for every column present in both tables (optionally filtered with a `WHERE` clause)
- Logging all results to a timestamped log file
- Works with Snowflake **SSO (external browser)** authentication

---

## 🧱 Files

| File                | Description                                         |
|---------------------|-----------------------------------------------------|
| `compare_tables.py` | Command-line script to compare two tables           |
| `compare_tables.ipynb` | Jupyter notebook version for interactive exploration |
| `example_config.json` | Sample configuration file for Snowflake connection |
| `requirements.txt`  | List of required Python packages                    |

---

## ⚙️ Setup

### 1. Install Dependencies

You need Python 3. You can install all required dependencies using:

```bash
pip install -r requirements.txt
```

Or, install just the Snowflake connector:

```bash
pip install snowflake-connector-python
```

If you want to use the Jupyter notebook version, also install Jupyter:

```bash
pip install notebook
```

### 2. Create Your Config File  
Make a copy of `example_config.json` and name it `config.json`:

```json
{
  "account": "YOUR_SNOWFLAKE_ACCOUNT",
  "user": "YOUR_SNOWFLAKE_USER",
  "warehouse": "YOUR_SNOWFLAKE_WARESHOUSE"
}
```
Replace the values with your actual Snowflake credentials.  
**Note:** This script uses externalbrowser authentication (SSO), so no password is required.

---

## 🚀 How to Use

### 🐍 Option 1: Run Python Script

Before running, update the table names (`TABLE_1` and `TABLE_2`) and, if needed, the `EXCLUDED_COLUMNS` list in `compare_tables.py` or `compare_tables.ipynb` to match your use case.

You can use the `WHERE_1` and `WHERE_2` variables to filter rows for each table if necessary.

```bash
python compare_tables.py
```

### 📓 Option 2: Use Jupyter Notebook

Open `compare_tables.ipynb`, update the table names and excluded columns as needed, and run the cells interactively.

---

## 📝 Output

A log file is created automatically, named like:

```
compare_log_2025-06-23_14-30-00.txt
```

It includes:

- Row count comparison (with any applied WHERE clause)
- Column mismatches
- Distinct value differences
- Summary of matching columns

---

## 🧼 Excluded Columns

You can customize which columns to exclude from comparison:

```python
EXCLUDED_COLUMNS = ['_FIVETRAN_SYNCED', '_MODIFIED', ...]
```

---

## ✅ Requirements

- Python 3.7+
- Access to Snowflake via externalbrowser (SSO)
- Necessary privileges to read metadata and query both tables

---

## 📬 Questions?

Feel free to open an issue or submit a pull request.