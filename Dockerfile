FROM python:3.11-rc-buster

WORKDIR /app
COPY app.py app.py

CMD python /app/app.py
