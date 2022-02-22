FROM python:3.9-slim
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update && apt-get -y install gcc postgresql-client libpq-dev
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
COPY python ./python
ENV PYTHONPATH ./python
COPY bash ./bash
RUN chmod +x ./bash/benchmark.sh
