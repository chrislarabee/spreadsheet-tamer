FROM python:3

WORKDIR /usr/src/datagenius

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY datagenius/ datagenius/
COPY tests/ tests/

CMD ["pytest"]
