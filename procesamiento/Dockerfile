FROM python:3.8

WORKDIR /app
COPY req.txt .

RUN pip install -r req.txt
RUN mkdir /app/logs
COPY procesamiento.py .

CMD ["python","procesamiento.py"]
