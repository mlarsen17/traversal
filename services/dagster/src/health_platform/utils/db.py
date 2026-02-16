from datetime import datetime

from sqlalchemy import text


def insert_bootstrap_heartbeat(engine, message: str, created_at: datetime):
    stmt = text(
        """
        INSERT INTO bootstrap_heartbeat (created_at, message)
        VALUES (:created_at, :message)
        """
    )
    with engine.begin() as conn:
        conn.execute(stmt, {"created_at": created_at, "message": message})
