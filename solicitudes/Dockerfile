FROM python:3.8

WORKDIR /app
# Instalar dependencias
COPY req.txt .
COPY dataset.json .

RUN pip install -r req.txt
COPY app.py .


CMD ["python","-u" ,"app.py"]



