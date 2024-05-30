FROM python:3.7

WORKDIR /app
COPY requirements.txt /app

RUN python -m pip install --upgrade pip && python -m pip install -r requirements.txt

COPY . /app

CMD ["python", "app.py"]