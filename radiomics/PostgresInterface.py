import logging
from time import sleep

import psycopg2
from global_var import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS

logger = logging.getLogger(__name__)


class PostgresInterface:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
        self.port = port

    def connect(self):
        """Connect to the PostgreSQL database."""
        for attempt in range(NUMBER_ATTEMPTS):
            try:
                self.conn = psycopg2.connect(
                    host=self.host, database=self.database, user=self.user, password=self.password, port=self.port
                )
                self.cursor = self.conn.cursor()
                logger.info("Connection established.")
                break
            except psycopg2.OperationalError as e:
                if attempt < NUMBER_ATTEMPTS - 1:
                    logger.warning("%s", e)
                    logger.info("Retrying in %s seconds...", RETRY_DELAY_IN_SECONDS)
                    sleep(RETRY_DELAY_IN_SECONDS)
                else:
                    raise Exception("Unable to connect to the database after time.") from e

    def disconnect(self):
        """Close the connection to the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed.")

    def execute_query(self, query, params=None):
        """Execute a query (e.g., INSERT, UPDATE, DELETE)."""
        try:
            self.cursor.execute(query, params)
            self.conn.commit()  # Commit changes to the database
            logger.info("Query executed successfully.")
        except Exception as e:
            self.conn.rollback()  # Rollback in case of error
            logger.warning("Error executing query: %s", e)
            raise

    def fetch_all(self, query, params=None):
        """Fetch all results from a SELECT query."""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            logger.warning("Error fetching results: %s", e)
            return None

    def fetch_one(self, query, params=None):
        """Fetch a single result from a SELECT query."""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchone()
        except Exception as e:
            logger.warning("Error fetching result: %s", e)
            return None

    def create_table(self, table_name, columns):
        """Create a table."""
        columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        self.execute_query(query)

    def insert(self, table_name, data):
        if not data:
            raise ValueError("No data provided for insert.")
        columns = list(data.keys())
        values = [data[col] for col in columns]  # ensures order matches columns
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
        self.execute_query(query, tuple(values))

    def update(self, table_name, data, where_conditions):
        """Update a row in a table."""
        set_clause = ", ".join([f"{col} = %s" for col in data])
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.execute_query(query, tuple(data.values()) + tuple(where_conditions.values()))

    def delete(self, table_name, where_conditions):
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.execute_query(query, tuple(where_conditions.values()))

    def check_table_exists(self, table_name):
        """Check if a table exists in the database."""
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = %s
        );
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchone()[0]
