FROM python:3.10

WORKDIR /flask 

ADD requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED True

CMD python3 app.py