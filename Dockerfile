FROM python:3.7-slim-buster

RUN /usr/local/bin/python -m pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY main.py ./main.py
COPY table.py ./table.py
COPY metadata.py ./metadata.py
COPY table_metadata.yaml ./table_metadata.yaml
CMD ["python", "main.py"]