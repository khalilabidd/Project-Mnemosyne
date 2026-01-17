#  SCD Type 2 Manager

This project provides a lightweight, file-based implementation combining Slowly Changing Dimension (SCD) Type 2 semantics using Parquet files and pandas.

**Features**
- SCD Type 2 versioning (effective_date, end_date, is_current, operation_date)
- Automatic schema history tracking ([parquet_warehouse/metadata/schema_history.json](parquet_warehouse/metadata/schema_history.json))
- Handles INSERT / UPDATE / DELETE and column additions/removals¨
- Row-level and column-level access control (ACLs) with simple JSON-backed stores


**Core class**
- `SCDManager` — implemented in the project script [scd2.py](scd2). It manages the SCD table under a base parquet directory (default used in examples: `parquet_warehouse`).

**Requirements**
- Python 3.8+
- pandas
- pyarrow or fastparquet (for Parquet read/write)

Install requirements (example):
```bash
pip install pandas pyarrow
```

**Quickstart**
1. Place or run the script in the workspace root.
2. Initialize the manager and call `process_daily_data()` with a pandas DataFrame, e.g.:

```python
from datetime import datetime
from scd2 import SCDManager
import pandas as pd

manager = SCDManager(base_path=r"parquet_warehouse", primary_keys=['customer_id','product_id'])

df = pd.DataFrame({
    'customer_id': [1,2],
    'product_id': ['A','B'],
    'amount': [100,200]
})

manager.process_daily_data(df, process_date=datetime(2026,1,1))
```

**Files & layout**
- SCD table: `parquet_warehouse/scd_type2_table.parquet`
- Schema history: [parquet_warehouse/metadata/schema_history.json](parquet_warehouse/
metadata/schema_history.json)
- Row permissions: `parquet_warehouse/metadata/row_permissions.json`
- Column permissions: `parquet_warehouse/metadata/column_permissions.json`

**Row- and column-level access control**
- The manager now supports simple ACLs:
  - set_row_permissions(key_values: Dict, allowed_users: List[str])
  - set_column_permissions(column: str, allowed_users: List[str])
- Querying functions accept optional access arguments:
  - get_current_state(user_id: Optional[str] = None, user_roles: Optional[List[str]] = None)
  - get_historical_view(as_of_date, user_id=None, user_roles=None)
  - get_record_history(key_values, user_id=None, user_roles=None)
- Rules:
  - If user_id is None -> full access (use restrictive behavior by passing a user id in production).
  - Users with 'admin' in user_roles bypass checks.
  - If row permissions are present, only rows whose primary-key tuple is allowed are returned.
  - If a column is not present in the column permissions store it is visible to all.
  - Primary key columns and SCD metadata columns are always retained in query results.

  **Permissions storage**
- ACLs are stored as small JSON files under the manager metadata dir:
  - row_permissions.json maps primary-key JSON tuples -> list of allowed users/roles.
  - column_permissions.json maps column name -> list of allowed users/roles.
- For production use:
  - Store permissions securely (restrict filesystem ACLs, encrypt at rest, or use a centralized authz service).

  **Examples**
```python
# Grant user alice access to a specific row
manager.set_row_permissions({'customer_id': 1, 'product_id': 'A'}, ['alice'])

# Grant role analysts access to the 'amount' column
manager.set_column_permissions('amount', ['analyst', 'alice'])

# Query current state as alice (will be filtered)
df = manager.get_current_state(user_id='alice', user_roles=['analyst'])
```

**Helper methods**
- `get_current_state()` — returns current (is_current==True) records
- `get_historical_view(as_of_date)` — returns table state as of a given date
- `get_record_history(key_values)` — full version history for a primary key

**Security & audit**
- The built-in ACLs are minimal and intended for lightweight/local control. Add audit logging (who changed ACLs, and access attempts) and strengthen storage/permissions before using in production.