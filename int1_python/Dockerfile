FROM python:3.12-slim

RUN pip install --no-cache-dir pytest mysql.connector

WORKDIR /pytest

COPY tests.py ./

CMD ["pytest", "tests.py", "-sv"]
