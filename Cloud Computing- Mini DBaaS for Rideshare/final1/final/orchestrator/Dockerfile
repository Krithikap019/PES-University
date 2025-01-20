FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

COPY ./requirements.txt /app/requirements.txt
COPY ./app.py /app/app.py

WORKDIR /app

RUN pip3 install kazoo

RUN pip3 install pika && \
    pip3 install -r requirements.txt 

RUN pip3 install -U Flask-SQLAlchemy && \
    pip3 install flask-marshmallow && \
    pip3 install marshmallow-sqlalchemy && \
    pip3 install flask-session && \
    pip3 install flask-restful


COPY . /app
RUN chmod +x /app

ENTRYPOINT ["python3"]
CMD ["app.py"]

