FROM python:latest


# Instalar dependencias
COPY requirements.txt /app/
COPY dataset.json /app/

WORKDIR /app
RUN pip install -r requirements.txt


CMD ["python","-u" ,"app.py"]
