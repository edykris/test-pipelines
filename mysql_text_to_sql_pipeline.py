import mysql.connector
import logging
import os
import re
import traceback
from typing import List, Union, Generator, Iterator
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mysql_pipeline")


class Pipeline:
    class Valves(BaseModel):
        pipelines: List[str] = ["*"]
        DB_HOST: str = os.getenv("MYSQL_HOST", "mysql")
        DB_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
        DB_USER: str = os.getenv("MYSQL_USER", "root")
        DB_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "root")
        DB_DATABASE: str = os.getenv("MYSQL_DB", "testdb")
        DB_TABLES: List[str] = ["tbl_barang"]

    def __init__(self):
        self.name = "MySQL Text→SQL Pipeline"
        self.conn = None
        self.cur = None
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("MYSQL_HOST", "mysql"),
                "DB_PORT": int(os.getenv("MYSQL_PORT", "3306")),
                "DB_USER": os.getenv("MYSQL_USER", "root"),
                "DB_PASSWORD": os.getenv("MYSQL_PASSWORD", "root"),
                "DB_DATABASE": os.getenv("MYSQL_DB", "testdb"),
                "DB_TABLES": ["tbl_barang"],
            }
        )

    def init_db_connection(self):
        """Tes koneksi awal + tampilkan tabel di database"""
        try:
            self.conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=int(self.valves.DB_PORT),
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE,
                connection_timeout=5
            )
            logger.info("✅ Connected to MySQL")

            cur = self.conn.cursor()
            cur.execute("SHOW TABLES;")
            tables = cur.fetchall()
            logger.info(f"Tables in {self.valves.DB_DATABASE}: {[t[0] for t in tables]}")
            cur.close()

            self.conn.close()
            self.conn = None
        except Exception as e:
            logger.error("❌ Error connecting to MySQL: %s", e)
            logger.debug(traceback.format_exc())

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass

    def _sanitize_and_prepare_sql(self, user_sql: str) -> str:
        """Izinkan hanya SELECT, tambahkan LIMIT kalau tidak ada"""
        if not user_sql or not isinstance(user_sql, str):
            raise ValueError("Empty SQL")

        low = user_sql.strip().lower()
        forbidden = ["insert ", "update ", "delete ", "drop ", "truncate ", "alter ", "create ", "replace ", "grant ", "revoke "]
        for kw in forbidden:
            if kw in low:
                raise ValueError(f"Forbidden SQL keyword detected: {kw.strip()}")

        if not re.match(r'^\s*(with\s+.*select|select)\b', low, re.I | re.S):
            raise ValueError("Only SELECT queries are allowed.")

        if 'limit' not in low:
            user_sql = user_sql.rstrip().rstrip(';') + " LIMIT 100"

        return user_sql + ";"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """
        Jalankan query SQL yang diberikan user_message
        """
        try:
            safe_sql = self._sanitize_and_prepare_sql(user_message.strip())

            conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=int(self.valves.DB_PORT),
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE,
                connection_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute(safe_sql)
            rows = cursor.fetchall()
            desc = [d[0] for d in cursor.description] if cursor.description else []
            cursor.close()
            conn.close()

            results = [dict(zip(desc, row)) for row in rows] if desc else [tuple(r) for r in rows]
            return str(results)

        except ValueError as ve:
            logger.warning("Validation error: %s", ve)
            return f"Validation error: {ve}"
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            logger.debug(traceback.format_exc())
            try:
                conn.close()
            except:
                pass
            return f"Unexpected error: {e}"
