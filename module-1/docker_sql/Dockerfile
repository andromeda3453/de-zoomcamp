FROM python:3.11.3

RUN apt-get install wget

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# RUN pip install pandas
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py"]