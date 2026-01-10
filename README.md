# Hybrid SCD Type 2 + Delta Manager

This project provides a lightweight, file-based implementation combining Slowly Changing Dimension (SCD) Type 2 semantics with Delta-style changelog recording using Parquet files and pandas.

**Features**
- SCD Type 2 versioning (effective_date, end_date, is_current)
- Delta-style changelog (operation, operation_date)
- Automatic schema history tracking ([parquet_warehouse/metadata/schema_history.json](parquet_warehouse/metadata/schema_history.json))
- Handles INSERT / UPDATE / DELETE and column additions/removals

**Core class**
- `HybridSCDDeltaManager` — implemented in the project script [scd2.py](scd2). It manages the SCD table and changelog under a base parquet directory (default used in examples: `parquet_warehouse`).

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
from scd2 import HybridSCDDeltaManager
import pandas as pd

manager = HybridSCDDeltaManager(base_path=r"parquet_warehouse", primary_keys=['customer_id','product_id'])

df = pd.DataFrame({
    'customer_id': [1,2],
    'product_id': ['A','B'],
    'amount': [100,200]
})

manager.process_daily_data(df, process_date=datetime(2026,1,1))
```

**Files & layout**
- SCD table: `parquet_warehouse/scd_type2_table.parquet`
- Changelog: `parquet_warehouse/changelog.parquet`
- Schema history: [parquet_warehouse/metadata/schema_history.json](parquet_warehouse/metadata/schema_history.json)

**Helper methods**
- `get_current_state()` — returns current (is_current==True) records
- `get_historical_view(as_of_date)` — returns table state as of a given date
- `get_record_history(key_values)` — full version history for a primary key
- `get_changelog()` — query recorded operations