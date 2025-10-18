KennelOS Analytics
===================

This project contains a simple ETL pipeline that extracts data from local files, transforms and validates the data, writes outputs to CSV/JSON, and now also persists results to a local SQLite database. A Streamlit dashboard is included to visualize the results.

Quick start
-----------

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run the ETL pipeline:

```bash
python main.py
```

After running, you'll find CSV/JSON outputs in the `output/` directory and a SQLite DB at `output/kennelos.db`.

3. Launch the dashboard:

```bash
streamlit run dashboard/app.py
```

Notes
-----
- To use a different database (e.g., Postgres), set the `DATABASE_URL` environment variable before running the pipeline or dashboard. The code uses SQLAlchemy and will respect the `DATABASE_URL` value.
- The dashboard expects the ETL to have created tables named `daily_summary`, `pet_activities`, `environment`, and `staff_logs` in the database.

Next steps
----------
- Add migrations (Alembic) for production DB schema management.
- Add tests for DB writes and dashboard components.
- Secure deployment for Streamlit (e.g., behind Auth Proxy).
