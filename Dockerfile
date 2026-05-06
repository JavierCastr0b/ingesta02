FROM python:3-slim

WORKDIR /programas/ingesta02

RUN pip3 install boto3
RUN pip3 install mysql-connector-python

COPY . .

CMD ["python3", "./ingesta.py"]
