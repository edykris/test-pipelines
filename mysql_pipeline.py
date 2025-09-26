# pipelines/mysql_pipeline.py

import mysql.connector
from mysql.connector import Error
import logging
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel

import aiohttp
import asyncio

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]

    def __init__(self):
        self.name = "02 Database Query (MySQL)"
        self.conn = None
        self.cursor = None
        self.nlsql_response = ""

        # Valves: semua default bisa diubah dari UI
        self.valves = self.Valves(
            **{
                "DB_HOST": os.getenv("MYSQL_HOST", "10.30.164.243"),
                "DB_PORT": os.getenv("MYSQL_PORT", "3306"),
                "DB_USER": os.getenv("MYSQL_USER", "admin"),
                "DB_PASSWORD": os.getenv("MYSQL_PASSWORD", "tester123"),
                "DB_DATABASE": os.getenv("MYSQL_DB", "testdb"),
                "DB_TABLES": ["movies"],
            }
        )

    def init_db_connection(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=self.valves.DB_PORT,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )
            if self.conn.is_connected():
                print("Connection to MySQL established successfully")

            self.cursor = self.conn.cursor()

            # Query untuk ambil daftar tabel
            self.cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                """,
                (self.valves.DB_DATABASE,)
            )

            tables = self.cursor.fetchall()
            print("Tables in the database:")
            for schema, table in tables:
                print(f"{schema}.{table}")

        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: List[dict],
        body: dict
    ) -> Union[str, Generator, Iterator]:
        try:
            conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=self.valves.DB_PORT,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )
            cursor = conn.cursor()
            cursor.execute(user_message)
            result = cursor.fetchall()

            cursor.close()
            conn.close()
            return str(result)

        except Error as e:
            logging.error(f"MySQL Error: {e}")
            if conn and conn.is_connected():
                conn.close()
            return f"MySQL Error: {e}"
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            if conn and conn.is_connected():
                conn.close()
            return f"Unexpected error: {e}"
