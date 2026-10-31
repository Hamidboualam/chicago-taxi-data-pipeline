FROM python:3.11-slim

RUN pip install --no-cache-dir streamlit==1.45.1 duckdb==1.3.2

# Extension DuckDB pour lire les fichiers sur S3
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY app.py /app/app.py

CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false", "--client.toolbarMode=minimal"]
