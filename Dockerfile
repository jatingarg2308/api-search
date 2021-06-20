FROM python:3.7-slim-buster

RUN /usr/local/bin/python -m pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . ./
EXPOSE 5000
CMD ["python", "app.py"]