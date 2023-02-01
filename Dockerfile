# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
FROM python:3.10

COPY ./app /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install system dependencies
RUN apt-get update \
  && apt-get -y install netcat gcc postgresql nano \
  && apt-get -y install curl \
  && apt-get clean

# install python dependencies
RUN pip install --upgrade pip
RUN pip install -r app/requirements.txt

CMD tail /dev/null -f