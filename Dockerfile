FROM python:3.11-slim-bookworm

# Java 17 (supporté par Spark 3.5) + police Unicode pour les fiches PDF.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless fonts-dejavu-core procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Pré-chargement des jars Kafka / Delta / GraphFrames : démo possible hors ligne.
COPY fraud/__init__.py fraud/config.py fraud/spark_session.py fraud/
RUN SPARK_IVY_DIR=/opt/ivy python -m fraud.spark_session --warmup \
 && ls /opt/ivy/jars
ENV SPARK_JARS_DIR=/opt/ivy/jars

COPY . .
