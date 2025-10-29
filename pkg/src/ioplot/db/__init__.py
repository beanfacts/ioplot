import sqlite3

from typing import List
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from iorpyd import IOROutput

class BenchmarkRun(BaseModel):
    series_id: str
    start_time: datetime
    cmd: List[str]
    ior_output: IOROutput = Field(alias="ior_result")
    
    @field_validator("start_time", mode="before")
    @classmethod
    def parse_start_time(cls, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        elif type(value) in (int, float):
            return datetime.fromtimestamp(value)
        return value
    
    
class BenchmarkDB:
    
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute(self._get_tbl_create_statement())
        self.table_name = "benchmark_runs"
    
    def _get_sql_insert_template(self) -> str:
        return f"""
            INSERT INTO 
                {self.table_name}(series_id, start_time, cmd, ior_output)
            VALUES 
                (?, ?, ?, ?)
        """
    
    def _get_tbl_create_statement(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                series_id TEXT,
                start_time TIMESTAMP,
                cmd TEXT,
                ior_output TEXT
            )
        """
        
    def insert_run(self, run: BenchmarkRun):
        insert_template = self._get_sql_insert_template()
        self.cursor.execute(
            insert_template,
            (
                run.series_id,
                run.start_time.isoformat(),
                " ".join(run.cmd),
                run.ior_output.model_dump_json(),
            )
        )
        self.conn.commit()
        
    def get_all_runs(self, series_id: str) -> List[BenchmarkRun]:
        select_template = f"""
            SELECT series_id, start_time, cmd, ior_output
            FROM {self.table_name}
            WHERE series_id = ?
        """
        self.cursor.execute(select_template, (series_id,))
        rows = self.cursor.fetchall()
        runs = []
        for row in rows:
            run = BenchmarkRun(
                series_id=row[0],
                start_time=row[1],
                cmd=row[2].split(" "),
                ior_result=IOROutput.model_validate_json(row[3]),
            )
            runs.append(run)
        return runs
        
    def close(self):
        self.conn.close()