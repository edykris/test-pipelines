import mysql.connector
import logging
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel

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
        self.name = "MySQL Database Query"
        self.conn = None
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("MYSQL_HOST", "localhost"),
                "DB_PORT": os.getenv("MYSQL_PORT", "3306"),
                "DB_USER": os.getenv("MYSQL_USER", "root"),
                "DB_PASSWORD": os.getenv("MYSQL_PASSWORD", "rootpassword"),
                "DB_DATABASE": os.getenv("MYSQL_DB", "testdb"),
                "DB_TABLES": ["tbl_barang"],
            }
        )

    def init_db_connection(self):
        connection_params = {
            'host': self.valves.DB_HOST,
            'port': int(self.valves.DB_PORT),
            'user': self.valves.DB_USER,
            'password': self.valves.DB_PASSWORD,
            'database': self.valves.DB_DATABASE
        }

        try:
            self.conn = mysql.connector.connect(**connection_params)
            print("✅ Connection to MySQL established successfully")
        except Exception as e:
            print(f"❌ Error connecting to MySQL: {e}")
            return

        # List tables
        cur = self.conn.cursor()
        cur.execute("SHOW TABLES;")
        tables = cur.fetchall()
        print("Tables in the database:")
        for t in tables:
            print(f"- {t[0]}")

        cur.close()
        self.conn.close()

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        if self.conn:
            self.conn.close()

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """ Jalankan query SQL yang diberikan user_message """
        try:
            conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=int(self.valves.DB_PORT),
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )
            cursor = conn.cursor()
            sql = user_message

            cursor.execute(sql)
            result = cursor.fetchall()

            cursor.close()
            conn.close()
            return str(result)

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            try:
                conn.close()
            except:
                pass
            return f"Unexpected error: {e}"
