# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /geo2ip
COPY requirements.txt /geo2ip/
RUN pip install -r requirements.txt
COPY . /geo2ip/
