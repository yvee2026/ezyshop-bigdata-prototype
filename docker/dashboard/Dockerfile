# Streamlit + Plotly dashboard reading MongoDB.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
COPY docker/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONPATH=/app
EXPOSE 8501
CMD ["streamlit", "run", "src/dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
