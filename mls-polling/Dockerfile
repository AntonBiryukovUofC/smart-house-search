FROM python:3.9.1-slim as mls-pip-packages

ADD requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.9.1-slim as mls-polling
COPY --from=mls-pip-packages /usr/local /usr/local
ADD ./src /opt/mls-polling/src
CMD ["python3", "-u", "/opt/mls-polling/src/app.py"]