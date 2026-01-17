import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import hashlib

class SCDManager:
    """
    SCD Type 2 implementation.
    Handles: INSERT, UPDATE, DELETE, column additions/removals
    """
    
    def __init__(self, base_path: str, primary_keys: List[str]):
        """
        Initialize the manager
        
        Args:
            base_path: Root directory for all parquet files
            primary_keys: List of column names forming the primary key
        """
        self.base_path = Path(base_path)
        self.primary_keys = primary_keys
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Directory structure
        # self.daily_snapshots_dir = self.base_path / "daily_snapshots"
        self.monthly_snapshots = False
        self.scd_type2_path = self.base_path / "scd_type2_table.parquet"
        self.metadata_dir = self.base_path / "metadata"
        self.schema_history_path = self.metadata_dir / "schema_history.json"
        
        # Create directories
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        # self.daily_snapshots_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize schema history
        self._initialize_schema_history()
    
    def _initialize_schema_history(self):
        """Initialize schema history tracking"""
        if not self.schema_history_path.exists():
            schema_history = {"versions": []}
            with open(self.schema_history_path, 'w') as f:
                json.dump(schema_history, f, indent=2)
        # initialize permissions store
        self.permissions_path = self.metadata_dir / "row_permissions.json"
        if not self.permissions_path.exists():
            with open(self.permissions_path, 'w') as f:
                json.dump({}, f, indent=2)
        # initialize column permissions store
        self.column_permissions_path = self.metadata_dir / "column_permissions.json"
        if not self.column_permissions_path.exists():
            with open(self.column_permissions_path, 'w') as f:
                json.dump({}, f, indent=2)

    def set_row_permissions(self, key_values: Dict, allowed_users: List[str]):
        """Grant access for a specific primary-key tuple to a list of users/roles."""
        key = json.dumps(key_values, sort_keys=True)
        with open(self.permissions_path, 'r') as f:
            perms = json.load(f)
        perms[key] = allowed_users
        with open(self.permissions_path, 'w') as f:
            json.dump(perms, f, indent=2)

    def set_column_permissions(self, column: str, allowed_users: List[str]):
        """Grant access to a specific column for a list of users/roles.
        If a column is not present in the permissions store it is visible to all.
        """
        with open(self.column_permissions_path, 'r') as f:
            col_perms = json.load(f)
        col_perms[column] = allowed_users
        with open(self.column_permissions_path, 'w') as f:
            json.dump(col_perms, f, indent=2)


    def _filter_df_by_user(self, df: pd.DataFrame, user_id: Optional[str] = None, user_roles: Optional[List[str]] = None) -> pd.DataFrame:
        """Filter rows and columns based on row- and column-level permissions.

        Rules:
          - If user_id is None -> full access.
          - Admin role bypasses checks.
          - If row permissions file is empty -> allow all rows.
          - If column not present in column permissions -> visible to all.
          - Primary key columns are always retained (required for row context).
        """
        if user_id is None:
            return df

        user_roles = user_roles or []
        if 'admin' in user_roles:
            return df

        # Load permissions safely
        try:
            with open(self.permissions_path, 'r') as f:
                row_perms = json.load(f)
        except:
            row_perms = {}

        try:
            with open(self.column_permissions_path, 'r') as f:
                col_perms = json.load(f)
        except:
            col_perms = {}

        # ROW filtering
        if row_perms:
            def row_key(row):
                return json.dumps({k: row[k] for k in self.primary_keys}, sort_keys=True)
            keys = df.apply(row_key, axis=1)
            def allowed_for_row(k):
                if k not in row_perms:
                    return False
                allowed = set(row_perms[k])
                if user_id in allowed:
                    return True
                if set(user_roles) & allowed:
                    return True
                return False
            allowed_mask = keys.apply(allowed_for_row)
            df = df[allowed_mask.values].reset_index(drop=True)

        # COLUMN filtering
        # Always keep primary key columns and essential metadata
        always_keep = set(self.primary_keys + ['effective_date', 'end_date', 'is_current', 'operation_date', 'row_hash'])
        visible_cols = []
        for col in df.columns:
            if col in always_keep:
                visible_cols.append(col)
                continue
            if not col_perms:
                visible_cols.append(col)
                continue
            if col not in col_perms:
                visible_cols.append(col)
                continue
            allowed = set(col_perms.get(col, []))
            if user_id in allowed or (set(user_roles) & allowed):
                visible_cols.append(col)
        # If no non-PK column remains, return at least PKs + metadata
        visible_cols = [c for c in df.columns if c in visible_cols]
        return df[visible_cols].reset_index(drop=True)
    
    def _get_row_hash(self, row: pd.Series, exclude_cols: Optional[List[str]] = None) -> str:
        """
        Generate hash of row content (excluding metadata columns)
        Used to detect changes
        """
        if exclude_cols is None:
            exclude_cols = ['effective_date', 'end_date', 'is_current', 
                           'row_hash', 'operation_date'] + self.primary_keys
        
        cols_to_hash = [col for col in row.index if col not in exclude_cols]
        values = '|'.join(str(row[col]) for col in cols_to_hash)
        return hashlib.md5(values.encode()).hexdigest()
    
    def _detect_schema_changes(self, new_data: pd.DataFrame, 
                               previous_schema: Optional[List[str]] = None) -> Dict:
        """
        Detect schema changes (new columns, removed columns, type changes)
        """
        current_schema = list(new_data.columns)
        exclude_cols = ['effective_date', 'end_date', 'is_current', 'row_hash', 'operation_date']
        if previous_schema is None:
            try:
                scd = pd.read_parquet(self.scd_type2_path)
                previous_schema = list(scd.columns)
            except:
                previous_schema = []

        schema_changes = {'timestamp': datetime.now().isoformat()}
        new_cols = [col for col in list(set(current_schema) - set(previous_schema)) if col not in exclude_cols]
        if new_cols:
            schema_changes['new_columns'] = new_cols
        removed_cols = [col for col in list(set(previous_schema) - set(current_schema)) if col not in exclude_cols]
        if removed_cols:
            schema_changes['removed_columns'] = removed_cols
        
        if schema_changes.get('new_columns') or schema_changes.get('removed_columns'):
            with open(self.schema_history_path, 'r') as f:
                history = json.load(f)
            history['versions'].append(schema_changes)
            with open(self.schema_history_path, 'w') as f:
                json.dump(history, f, indent=2)

        return schema_changes
    
    def _add_metadata_columns(self, df: pd.DataFrame, 
                             effective_date: datetime) -> pd.DataFrame:
        """Add SCD Type 2 and Delta metadata columns"""
        df_copy = df.copy()
        
        # SCD Type 2 columns
        if 'effective_date' not in df_copy.columns:
            df_copy['effective_date'] = effective_date.date()
        if 'end_date' not in df_copy.columns:
            df_copy['end_date'] = None
        if 'is_current' not in df_copy.columns:
            df_copy['is_current'] = True
        
        # Delta columns
        if 'operation_date' not in df_copy.columns:
            df_copy['operation_date'] = datetime.now()
        if 'row_hash' not in df_copy.columns:
            df_copy['row_hash'] = df_copy.apply(self._get_row_hash, axis=1)
        
        return df_copy
    
    def _align_schemas(self, current_df: pd.DataFrame, 
                      target_df: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns from target_df to current_df"""
        exclude_cols = ['effective_date', 'end_date', 'is_current', 'row_hash', 'operation_date']
        for col in target_df.columns:
            if col not in (list(current_df.columns) + exclude_cols):
                current_df[col] = None
                current_df[col] = current_df[col].astype(target_df[col].dtype)
        
        # Reorder to match target
        return current_df
    
    def _detect_changes(self, new_data: pd.DataFrame, 
                       previous_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Detect INSERT, UPDATE, DELETE operations
        
        Returns:
            (insertions, updates, deletions)
        """
        
        # Get current records from SCD
        current_records = previous_data[previous_data['is_current'] == True].copy()

        # Merge to detect changes
        merged = new_data[self.primary_keys + ['row_hash']].merge(
            current_records[self.primary_keys + ['row_hash']],
            on=self.primary_keys,
            how='outer',
            indicator=True,
            suffixes=('_new', '_old')
        )
        
        # Classify operations
        insertions = merged[merged['_merge'] == 'left_only'].copy()
        deletions = merged[merged['_merge'] == 'right_only'].copy()
        
        # Updates: records that exist in both but have different hashes
        updates = merged[merged['_merge'] == 'both'].copy()
        updates = updates[updates['row_hash_new'] != updates['row_hash_old']].copy()
        return insertions[self.primary_keys], updates[self.primary_keys], deletions[self.primary_keys]
    
    def process_daily_data(self, new_data: pd.DataFrame, 
                          process_date: Optional[datetime] = None) -> Dict:
        """
        Main entry point: Process daily data and maintain both SCD Type 2 and Delta log
        
        Args:
            new_data: DataFrame with new/updated data for the day
            process_date: Date of processing (defaults to today)
        
        Returns:
            Dictionary with processing statistics
        """
        if process_date is None:
            process_date = datetime.now()
        
        # Detect schema changes
        schema_changes = self._detect_schema_changes(new_data)
        
        # Store daily snapshot
        # daily_file = self.daily_snapshots_dir / f"data_{process_date.strftime('%Y%m%d')}.parquet"
        # new_data.to_parquet(daily_file, index=False)
        
        stats = {
            'process_date': process_date.isoformat(),
            'schema_changes': schema_changes,
            'insertions': 0,
            'updates': 0,
            'deletions': 0,
            'total_current_records': 0
        }
        
        # Load or create SCD Type 2 table
        try:
            scd_table = pd.read_parquet(self.scd_type2_path)
            if self.monthly_snapshots:
                mask = ((scd_table['effective_date'].dt.month == process_date.month) & \
                        (scd_table['effective_date'].dt.year == process_date.year)) | (scd_table['is_current'] == True)
                scd_table = scd_table.loc[mask]

        except:
            scd_table = pd.DataFrame()
        
        # Handle schema alignment
        if not scd_table.empty:
            new_data = self._align_schemas(new_data, scd_table)
            scd_table = self._align_schemas(scd_table, new_data)
            new_data['row_hash'] = new_data.apply(self._get_row_hash, axis=1)
            if schema_changes.get('new_columns') or schema_changes.get('removed_columns'):
                scd_table['row_hash'] = scd_table.apply(self._get_row_hash, axis=1)
        
        # Detect changes if SCD table exists
        if not scd_table.empty:
            insertion_keys, update_keys, deletion_keys = self._detect_changes(new_data, scd_table)
            mask = new_data.set_index(self.primary_keys).index.isin(
                    insertion_keys.set_index(self.primary_keys).index) | new_data.set_index(self.primary_keys).index.isin(
                    update_keys.set_index(self.primary_keys).index)
            new_data = self._add_metadata_columns(new_data.loc[mask], process_date)
            stats['insertions'] = len(insertion_keys)
            stats['updates'] = len(update_keys)
            stats['deletions'] = len(deletion_keys)
            
            # Process DELETIONS: Mark as inactive
            if not deletion_keys.empty:
                mask = scd_table.set_index(self.primary_keys).index.isin(
                    deletion_keys.set_index(self.primary_keys).index
                ) & (scd_table['is_current'] == True)
                scd_table.loc[mask, 'end_date'] = process_date.date()
                scd_table.loc[mask, 'is_current'] = False
                
            # Process UPDATES: Deactivate old, add new with version
            if not update_keys.empty:
                mask = scd_table.set_index(self.primary_keys).index.isin(
                    update_keys.set_index(self.primary_keys).index
                ) & (scd_table['is_current'] == True)
                scd_table.loc[mask, 'end_date'] = process_date.date()
                scd_table.loc[mask, 'is_current'] = False
                
                mask = new_data.set_index(self.primary_keys).index.isin(
                    update_keys.set_index(self.primary_keys).index
                )
                # Add updated records with new effective date
                scd_table = pd.concat([scd_table, new_data.loc[mask]], ignore_index=True)
                
            # Process INSERTIONS: Add new records
            if not insertion_keys.empty:
                mask = new_data.set_index(self.primary_keys).index.isin(
                    insertion_keys.set_index(self.primary_keys).index
                )
                scd_table = pd.concat([scd_table, new_data.loc[mask]], ignore_index=True)
        else:
            # First load: all records are insertions
            scd_table = self._add_metadata_columns(new_data, process_date)
            stats['insertions'] = len(scd_table)
        
        # Save updated SCD Type 2 table
        scd_table.to_parquet(self.scd_type2_path, index=False)
        if self.monthly_snapshots:
            scd_table.to_parquet(self.base_path / f"scd_type2_table_{process_date.strftime('%Y%m')}.parquet", index=False)

        
        # Update current record count
        stats['total_current_records'] = len(scd_table[scd_table['is_current'] == True])
        
        return stats

    def get_current_state(self, user_id: Optional[str] = None, user_roles: Optional[List[str]] = None) -> pd.DataFrame:
        """Get the current state of all records (is_current == True) with row/column-level filtering"""
        scd = pd.read_parquet(self.scd_type2_path)
        result = scd[scd['is_current'] == True].reset_index(drop=True)
        return self._filter_df_by_user(result, user_id, user_roles)
    
    def get_historical_view(self, as_of_date: datetime, user_id: Optional[str] = None, user_roles: Optional[List[str]] = None) -> pd.DataFrame:
        """Get data as it was on a specific date (filtered by user)."""
        scd = pd.read_parquet(self.scd_type2_path)
        
        # Records effective before or on the date and either no end_date or end_date after the date
        as_of = as_of_date.date()
        mask = (scd['effective_date'] <= as_of) & (
            (scd['end_date'].isna()) | (scd['end_date'] > as_of)
        )
        
        result = scd[mask].reset_index(drop=True)
        return self._filter_df_by_user(result, user_id, user_roles)
    
    def get_record_history(self, key_values: Dict, user_id: Optional[str] = None, user_roles: Optional[List[str]] = None) -> pd.DataFrame:
        """Get complete history of a specific record by primary key (filtered by user)."""
        scd = pd.read_parquet(self.scd_type2_path)
        
        mask = pd.Series([True] * len(scd), index=scd.index)
        for key, value in key_values.items():
            mask &= (scd[key] == value)
        
        result = scd[mask].sort_values('effective_date').reset_index(drop=True)
        return self._filter_df_by_user(result, user_id, user_roles))
    
    def get_statistics(self) -> Dict:
        """Get overall statistics"""
        scd = pd.read_parquet(self.scd_type2_path)
        
        return {
            'total_current_records': len(scd[scd['is_current'] == True]),
            'total_historical_records': len(scd),
            'schema_versions': len(json.load(open(self.schema_history_path))['versions'])
        }


# Example usage
if __name__ == "__main__":
    # Initialize manager
    manager = SCDManager(
        base_path=r"C:\Users\khali\projects\Project-Mnemosyne",
        primary_keys=['customer_id', 'product_id']
    )
    
    # Day 1: Initial data load
    print("=" * 60)
    print("DAY 1: Initial Data Load")
    print("=" * 60)
    day1_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'product_id': ['A', 'B', 'C'],
        'amount': [100, 200, 300],
        'status': ['active', 'active', 'active']
    })
    
    stats = manager.process_daily_data(day1_data, process_date=datetime(2026, 1, 1))
    print(f"Insertions: {stats['insertions']}")
    print(f"Updates: {stats['updates']}")
    print(f"Deletions: {stats['deletions']}")
    print(f"Total current records: {stats['total_current_records']}\n")
    
    # Day 2: Some updates, new addition, deletion
    print("=" * 60)
    print("DAY 2: Updates, Insertions, Deletions")
    print("=" * 60)
    day2_data = pd.DataFrame({
        'customer_id': [1, 2, 4],  # Customer 3 deleted, Customer 4 added
        'product_id': ['A', 'B', 'D'],
        'amount': [150, 200, 400],  # Customer 1 amount changed
        'status': ['active', 'inactive', 'active']  # Customer 2 status changed
    })
    
    stats = manager.process_daily_data(day2_data, process_date=datetime(2026, 1, 2))
    print(f"Insertions: {stats['insertions']}")
    print(f"Updates: {stats['updates']}")
    print(f"Deletions: {stats['deletions']}")
    print(f"Total current records: {stats['total_current_records']}")
    print(f"Schema changes: {stats['schema_changes']}\n")
    
    # Day 3: New column addition
    print("=" * 60)
    print("DAY 3: New Column Addition")
    print("=" * 60)
    day3_data = pd.DataFrame({
        'customer_id': [1, 2, 4, 5],
        'product_id': ['A', 'B', 'D', 'E'],
        'amount': [150, 200, 400, 500],
        'status': ['active', 'inactive', 'active', 'pending'],
        'discount_percent': [None, 0, 10, 15]  # New column
    })
    
    stats = manager.process_daily_data(day3_data, process_date=datetime(2026, 1, 3))
    print(f"Insertions: {stats['insertions']}")
    print(f"Updates: {stats['updates']}")
    print(f"Deletions: {stats['deletions']}")
    print(f"Total current records: {stats['total_current_records']}")
    print(f"Schema changes: {stats['schema_changes']}\n")
    
    # Query operations
    print("=" * 60)
    print("QUERIES")
    print("=" * 60)
    
    print("\nCurrent state:")
    print(manager.get_current_state())
    
    print("\nHistorical view as of 2026-01-02:")
    print(manager.get_historical_view(datetime(2026, 1, 2)))
    
    print("\nHistory of customer 1, product A:")
    print(manager.get_record_history({'customer_id': 1, 'product_id': 'A'}))
    
    print("\nStatistics:")
    for key, value in manager.get_statistics().items():
        print(f"{key}: {value}")