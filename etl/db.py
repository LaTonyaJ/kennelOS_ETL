"""
Simple DB helper for KennelOS ETL pipeline.
Uses SQLAlchemy engine and pandas.to_sql for quick local persistence.

Default: SQLite file at output/kennelos.db. Can be overridden with DATABASE_URL env var.
"""
from sqlalchemy import create_engine
from pathlib import Path
import os


def get_engine(db_url: str | None = None):
    """Return a SQLAlchemy engine. If db_url is None, use output/kennelos.db."""
    # Respect explicit parameter, then env var, then default sqlite in output
    if db_url:
        url = db_url
    else:
        url = os.environ.get('DATABASE_URL')

    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)

    if not url:
        # Use a local sqlite file inside output for easy local runs
        db_path = output_dir / 'kennelos.db'
        url = f"sqlite:///{db_path}"

    engine = create_engine(url, connect_args={'check_same_thread': False} if url.startswith('sqlite') else {})
    return engine
