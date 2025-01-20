FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

COPY ./requirements.txt /app/requirements.txt
COPY ./app.py /app/app.py
COPY ./docker-entrypoint.sh /app/docker-entrypoint.sh
COPY ./zook.py /app/zook.py
COPY ./constant.py /app/constant.py
RUN chmod +x /app/docker-entrypoint.sh
RUN chmod +x /app/zook.py
RUN chmod +x /app/constant.py

WORKDIR /app

RUN pip3 install kazoo
RUN pip3 install pika && \
    pip3 install -r requirements.txt
RUN pip3 install docker
RUN pip3 install flask
RUN pip3 install -U Flask-SQLAlchemy && \
    pip3 install flask-marshmallow && \
    pip3 install marshmallow-sqlalchemy && \
    pip3 install flask-session && \
    pip3 install flask-restful
COPY . /app
RUN chmod +x /app

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]

