"""
This module provides functions to connect to a Trino database and perform various operations.
"""

import trino
import streamlit as st
import pandas as pd


def init_connection() -> trino.dbapi.Connection:
    """Initializes a connection to Trino.
    Returns:
        trino.dbapi.Connection: A connection object to interact with Trino.
    """

    return trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="trino",
        catalog="iceberg",
        schema="default",
        http_scheme="http",
    )


def fetch_stats(cursor: trino.dbapi.Cursor, schema: str, table: str) -> dict:
    """Fetches statistics from a Trino table.
    Args:
        cursor (trino.dbapi.Cursor): A cursor object to execute queries.
        schema (str): The schema name.
        table (str): The table name.
    Returns:
        dict: A dictionary containing various statistics about the table.
    """
    queries = {
        "Files": f'SELECT COUNT(*) FROM {schema}."{table}$files"',
        "Partitions": f'SELECT COUNT(*) FROM {schema}."{table}$partitions"',
        "Rows": f'SELECT COUNT(*) FROM {schema}."{table}"',
        "Snapshots": f'SELECT COUNT(*) FROM {schema}."{table}$snapshots"',
        "History": f'SELECT COUNT(*) FROM {schema}."{table}$history"',
        "Small Files (<100MB)": f'SELECT COUNT(*) FROM {schema}."{table}$files" WHERE file_size_in_bytes < 104857600',
        "Average File Size (MB)": f'SELECT ROUND(AVG(file_size_in_bytes)/1000000, 2) FROM {schema}."{table}$files"',
        "Largest File Size (MB)": f'SELECT ROUND(MAX(file_size_in_bytes)/1000000, 2) FROM {schema}."{table}$files"',
        "Smallest File Size (MB)": f'SELECT ROUND(MIN(file_size_in_bytes)/1000000, 2) FROM {schema}."{table}$files"',
        "Average Records per File": f'SELECT ROUND(AVG(record_count), 0) FROM {schema}."{table}$files"',
        "Std Dev File Size (MB)": f'SELECT ROUND(STDDEV_POP(file_size_in_bytes)/1000000, 2) FROM {schema}."{table}$files"',
        "Variance File Size (Bytes²)": f'SELECT ROUND(VAR_POP(file_size_in_bytes), 2) FROM {schema}."{table}$files"',
    }
    return {key: cursor.execute(query).fetchone()[0] for key, query in queries.items()}


def load_snapshot_history(cursor, schema, table):
    return pd.DataFrame(
        cursor.execute(f'''
        SELECT committed_at, snapshot_id, parent_id, operation, summary
        FROM {schema}."{table}$snapshots" ORDER BY committed_at DESC
    ''').fetchall(),
        columns=["Committed At", "Snapshot ID", "Parent ID", "Operation", "Summary"],
    )


def load_file_details(cursor, schema, table):
    return pd.DataFrame(
        cursor.execute(f'''
        SELECT content, file_format, file_path, CAST(record_count AS bigint), CAST(file_size_in_bytes AS bigint)
        FROM {schema}."{table}$files" ORDER BY file_size_in_bytes DESC
    ''').fetchall(),
        columns=["Content", "Format", "Path", "Records", "Size"],
    )


def execute_alter_table(cursor, schema, table, command):
    cursor.execute(f"ALTER TABLE {schema}.{table} EXECUTE {command}")
    st.success(f"Executed command: {command}")
