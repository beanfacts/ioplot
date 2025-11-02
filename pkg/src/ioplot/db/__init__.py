import json
import sqlite3
import logging

from typing import List, Dict
from datetime import datetime

from pydantic import BaseModel, Field, field_validator
from pydantic import ValidationError

from iorpyd import IOROutput


class BenchmarkRun(BaseModel):
    series_id: str
    start_time: datetime
    cmd: List[str]
    ior_output: IOROutput = Field(alias="ior_result")
    extra_data: Dict = Field(default_factory=dict)
    
    @field_validator("start_time", mode="before")
    @classmethod
    def parse_start_time(cls, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        elif type(value) in (int, float):
            return datetime.fromtimestamp(value)
        return value
    
    @field_validator("extra_data", mode="before")
    @classmethod
    def parse_json(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v


class BenchmarkDB:
    
    def __init__(self, db_path: str):
        self.table_name = "benchmark_runs"
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute(self._get_tbl_create_statement())
    
    def _get_sql_insert_template(self) -> str:
        return f"""
            INSERT INTO 
                {self.table_name}(series_id, start_time, cmd, ior_output, extra)
            VALUES 
                (?, ?, ?, ?, ?)
        """
    
    def _get_tbl_create_statement(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                series_id TEXT,
                start_time TIMESTAMP,
                cmd TEXT,
                ior_output TEXT,
                extra TEXT
            )
        """
        
    def insert_run(self, run: BenchmarkRun):
        self.cursor.execute(
            self._get_sql_insert_template(),
            (
                run.series_id,
                run.start_time.isoformat(),
                " ".join(run.cmd),
                run.ior_output.model_dump_json(),
                json.dumps(run.extra_data)
            )
        )
        self.conn.commit()
    
    def get_all_series(self) -> List[str]:
        select_template = f"""
            SELECT DISTINCT series_id
            FROM {self.table_name}
        """
        self.cursor.execute(select_template)
        rows = self.cursor.fetchall()
        return [row[0] for row in rows]
       
    def get_all_runs(self, series_id: str) -> List[BenchmarkRun]:
        select_template = f"""
            SELECT series_id, start_time, cmd, ior_output, extra
            FROM {self.table_name}
            WHERE series_id = ?
        """
        self.cursor.execute(select_template, (series_id,))
        rows = self.cursor.fetchall()
        runs = []
        for row in rows:
            try:
                run = BenchmarkRun(
                    series_id=row[0],
                    start_time=row[1],
                    cmd=row[2].split(" "),
                    ior_result=IOROutput.model_validate_json(row[3]),
                    extra_data=row[4]
                )
                runs.append(run)
            except ValidationError as e:
                logging.warning(
                    "Failed to parse benchmark run: %s", e
                )
        return runs
        
    def close(self):
        self.conn.close()