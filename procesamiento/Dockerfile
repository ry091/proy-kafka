FROM python:3.8

WORKDIR /app
COPY req.txt .

RUN pip install -r req.txt

COPY app.py .

CMD ["python","app.py"]
