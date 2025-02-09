import pandas as pd
import psycopg2
from typing import Any, Dict
import os

class CsvDataIngestion:
    def __init__(
        self,
        db_config: Dict[str, str],
        table_name: str = None,
        csv_path: str = None,
        append: bool = False,
        schema_match_threshold: float = 0.6
    ):
        self.db_config = db_config.copy()
        self.schema = self.db_config.pop('schema', 'public')
        self.csv_path = csv_path
        self.append = append
        self.schema_match_threshold = schema_match_threshold
        self.table_name = table_name
        
        if csv_path:
            self.df = self._load_csv()
            self.csv_columns = list(self.df.columns)
            
            if not self.append:
                self._drop_table_if_exists()
            self._create_table_with_id()
            self._insert_data()

    def _connect_postgres(self):
        return psycopg2.connect(**self.db_config)

    def _load_csv(self) -> pd.DataFrame:
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
            
        try:
            df = pd.read_csv(self.csv_path)
            if df.empty:
                raise pd.errors.EmptyDataError("CSV file is empty")
            return df
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing CSV file: {str(e)}")

    def _drop_table_if_exists(self):
        drop_query = f"DROP TABLE IF EXISTS {self.schema}.{self.table_name}"
        with self._connect_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(drop_query)
            conn.commit()

    def _create_table_with_id(self):
        if not hasattr(self, 'df') or self.df.empty:
            raise ValueError("No data loaded from CSV")

        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'NUMERIC',
            'object': 'TEXT',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }

        column_definitions = ["_csv_import_id_ SERIAL PRIMARY KEY"]
        
        for column in self.df.columns:
            pg_type = dtype_mapping.get(str(self.df[column].dtype), 'TEXT')
            column_definitions.append(f"\"{column}\" {pg_type}")

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
                {', '.join(column_definitions)}
            )
        """

        with self._connect_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
            conn.commit()

    def _insert_data(self):
        if self.df.empty:
            print("Warning: No data to insert")
            return

        columns = self.df.columns
        if not columns.empty:
            placeholders = ','.join(['%s'] * len(columns))
            columns_str = ','.join(f'"{col}"' for col in columns)
            
            insert_query = f"""
                INSERT INTO {self.schema}.{self.table_name} 
                ({columns_str}) 
                VALUES ({placeholders})
            """

            with self._connect_postgres() as conn:
                with conn.cursor() as cur:
                    for _, row in self.df.iterrows():
                        cur.execute(insert_query, tuple(row))
                conn.commit()

    def get_sample_data(self, limit: int = 10) -> pd.DataFrame:
        if not self.table_name:
            raise ValueError("No table selected")
            
        query = f"SELECT * FROM {self.schema}.{self.table_name} LIMIT {limit}"
        
        with self._connect_postgres() as conn:
            return pd.read_sql(query, conn)

    def search_data(
        self,
        conditions: Dict[str, Any],
        operator: str = 'AND',
        case_sensitive: bool = False
    ) -> pd.DataFrame:
        if not self.table_name:
            raise ValueError("No table selected for searching")
            
        if not conditions:
            raise ValueError("No search conditions provided")

        where_clauses = []
        params = []
        
        for col, value in conditions.items():
            if isinstance(value, str) and not case_sensitive:
                where_clauses.append(f"LOWER(\"{col}\") = LOWER(%s)")
            else:
                where_clauses.append(f"\"{col}\" = %s")
            params.append(value)

        where_statement = f" {operator} ".join(where_clauses)
        query = f"""
            SELECT * 
            FROM {self.schema}.{self.table_name} 
            WHERE {where_statement}
        """

        with self._connect_postgres() as conn:
            return pd.read_sql(query, conn, params=params)

def test_csv_ingestion():
    # Configure database connection
    db_config = {
        'dbname': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_DB'),
        'user': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_USER'),
        'password': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_PASSWORD'),
        'host': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_HOST'),
        'port': int(os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_PORT')),
        'schema': os.getenv('HOME_PROD_ENV_STAGING_POSTGRES_TEST_SCHEMA')
    }

    try:
        # Create sample DataFrame
        sample_data = {
            'name': ['John', 'Jane', 'Bob'],
            'age': [30, 25, 35],
            'city': ['New York', 'Los Angeles', 'Chicago']
        }
        sample_df = pd.DataFrame(sample_data)
        
        # Save to temporary CSV
        sample_csv_path = '__tests__/testdata/csv_data_table01.csv'
        sample_df.to_csv(sample_csv_path, index=False)

        # Initialize CsvDataIngestion
        ingestion = CsvDataIngestion(
            db_config=db_config,
            table_name="csv_data_table01",
            csv_path=sample_csv_path
        )

        # Test get_sample_data
        print("\nSample data:")
        sample_data = ingestion.get_sample_data(limit=5)
        print(sample_data)

        # Test case-insensitive search
        print("\nCase-insensitive search results:")
        search_results = ingestion.search_data(
            {'name': 'john'}, 
            case_sensitive=False
        )
        print(search_results)

        # Test case-sensitive search
        print("\nCase-sensitive search results:")
        search_results = ingestion.search_data(
            {'name': 'John'}, 
            case_sensitive=True
        )
        print(search_results)

        # Clean up
        os.remove(sample_csv_path)
        print("\nTest completed successfully!")

    except Exception as e:
        print(f"Error during test: {str(e)}")
        if os.path.exists(sample_csv_path):
            os.remove(sample_csv_path)

if __name__ == "__main__":
    test_csv_ingestion()