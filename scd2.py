import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import hashlib

class HybridSCDDeltaManager:
    """
    Hybrid approach combining SCD Type 2 with Delta Lake principles.
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
        self.changelog_path = self.base_path / "changelog.parquet"
        
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
                
                # Log deletions
                self._log_delta_operation(scd_table.loc[mask], process_date, 'DELETE')
            
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
                
                # Log updates
                self._log_delta_operation(new_data.loc[mask], process_date, 'UPDATE')
            
            # Process INSERTIONS: Add new records
            if not insertion_keys.empty:
                mask = new_data.set_index(self.primary_keys).index.isin(
                    insertion_keys.set_index(self.primary_keys).index
                )
                scd_table = pd.concat([scd_table, new_data.loc[mask]], ignore_index=True)
                
                # Log insertions
                self._log_delta_operation(new_data.loc[mask], process_date, 'INSERT')
        else:
            # First load: all records are insertions
            scd_table = self._add_metadata_columns(new_data, process_date)
            stats['insertions'] = len(scd_table)
            self._log_delta_operation(scd_table, process_date, 'INSERT')
        
        # Save updated SCD Type 2 table
        scd_table.to_parquet(self.scd_type2_path, index=False)
        if self.monthly_snapshots:
            scd_table.to_parquet(self.base_path / f"scd_type2_table_{process_date.strftime('%Y%m')}.parquet", index=False)

        
        # Update current record count
        stats['total_current_records'] = len(scd_table[scd_table['is_current'] == True])
        
        return stats

    def _log_delta_operation(self, data: pd.DataFrame, process_date: datetime, operation_type: str):
        """Log operations to delta log"""
        # Load existing changelog
        try:
            changelog = pd.read_parquet(self.changelog_path)
            if self.monthly_snapshots:
                mask = ((changelog['effective_date'].dt.month == process_date.month) & \
                        (changelog['effective_date'].dt.year == process_date.year)) | (changelog['is_current'] == True)
                changelog = changelog.loc[mask]

        except:
            changelog = pd.DataFrame()
        
        # Prepare log entry
        data_copy = data.copy()
        data_copy['operation'] = operation_type

        
        # Align columns
        if not changelog.empty:
            changelog = self._align_schemas(changelog, data_copy)
            data_copy = self._align_schemas(data_copy, changelog)
        
        # Append to changelog
        changelog = pd.concat([changelog, data_copy], ignore_index=True)
        changelog.to_parquet(self.changelog_path, index=False)
        if self.monthly_snapshots:
            changelog.to_parquet(self.base_path / f"changelog_{process_date.strftime('%Y%m')}.parquet", index=False)

    def get_current_state(self) -> pd.DataFrame:
        """Get the current state of all records (is_current == True)"""
        scd = pd.read_parquet(self.scd_type2_path)
        return scd[scd['is_current'] == True].reset_index(drop=True)
    
    def get_historical_view(self, as_of_date: datetime) -> pd.DataFrame:
        """Get data as it was on a specific date"""
        scd = pd.read_parquet(self.scd_type2_path)
        
        # Records effective before or on the date and either no end_date or end_date after the date
        as_of = as_of_date.date()
        mask = (scd['effective_date'] <= as_of) & (
            (scd['end_date'].isna()) | (scd['end_date'] > as_of)
        )
        
        return scd[mask].reset_index(drop=True)
    
    def get_record_history(self, key_values: Dict) -> pd.DataFrame:
        """Get complete history of a specific record by primary key"""
        scd = pd.read_parquet(self.scd_type2_path)
        
        mask = pd.Series([True] * len(scd), index=scd.index)
        for key, value in key_values.items():
            mask &= (scd[key] == value)
        
        return scd[mask].sort_values('effective_date').reset_index(drop=True)
    
    def get_changelog(self, operation_type: Optional[str] = None,
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None) -> pd.DataFrame:
        """Query the changelog"""
        changelog = pd.read_parquet(self.changelog_path)
        
        if operation_type:
            changelog = changelog[changelog['operation'] == operation_type]
        
        if start_date:
            changelog = changelog[changelog['operation_date'] >= start_date]
        
        if end_date:
            changelog = changelog[changelog['operation_date'] <= end_date]
        
        return changelog.reset_index(drop=True)
    
    def get_statistics(self) -> Dict:
        """Get overall statistics"""
        scd = pd.read_parquet(self.scd_type2_path)
        changelog = pd.read_parquet(self.changelog_path)
        
        return {
            'total_current_records': len(scd[scd['is_current'] == True]),
            'total_historical_records': len(scd),
            'total_operations_logged': len(changelog),
            'operations_by_type': changelog['operation'].value_counts().to_dict() if not changelog.empty else {},
            'schema_versions': len(json.load(open(self.schema_history_path))['versions'])
        }


# Example usage
if __name__ == "__main__":
    # Initialize manager
    manager = HybridSCDDeltaManager(
        base_path=r"c:\Users\khali\scripts\parquet_warehouse",
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
    
    print("\nChangelog:")
    print(manager.get_changelog())
    
    print("\nStatistics:")
    for key, value in manager.get_statistics().items():
        print(f"{key}: {value}")