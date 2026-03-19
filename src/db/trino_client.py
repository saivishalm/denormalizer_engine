"""
Trino Database Client
Manages connections and queries to Trino/Iceberg data warehouse.
"""

from trino.dbapi import connect
from trino.auth import BasicAuthentication
import logging

logger = logging.getLogger(__name__)


class TrinoClient:
    """
    Manages Trino database connections and query execution.
    Supports BasicAuthentication and parameterized queries.
    """
    
    def __init__(self, host: str, port: int, user: str, password: str, catalog: str):
        """
        Initialize Trino client with connection parameters.
        
        Args:
            host: Trino server hostname
            port: Trino server port
            user: Username for authentication
            password: Password for authentication
            catalog: Default catalog (Iceberg)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.connection = None
    
    def connect(self):
        """Establish connection to Trino server."""
        try:
            self.connection = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                auth=BasicAuthentication(self.user, self.password),
                catalog=self.catalog
            )
            logger.info(f"Connected to Trino at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise
    
    def execute_query(self, query: str, fetch_all: bool = False):
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            fetch_all: If True, fetch all rows; else return cursor
        
        Returns:
            Cursor or list of rows
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            if fetch_all:
                return cursor.fetchall()
            return cursor
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def fetch_table_data(self, schema: str, table: str, limit: int = None):
        """
        Fetch all rows from a table.
        Quotes schema and table names for identifiers starting with digits.
        
        Args:
            schema: Schema name
            table: Table name
            limit: Optional row limit
        
        Returns:
            List of tuples (rows)
        """
        schema_quoted = f'"{schema}"' if schema[0].isdigit() else schema
        table_quoted = f'"{table}"' if table[0].isdigit() else table
        limit_clause = f" LIMIT {limit}" if limit else ""
        query = f"SELECT * FROM {schema_quoted}.{table_quoted}{limit_clause}"
        return self.execute_query(query, fetch_all=True)
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Trino connection closed")
