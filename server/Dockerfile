FROM ubuntu:16.04

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /server/requirements.txt

WORKDIR /server

RUN pip install -r requirements.txt

COPY . /server

ENTRYPOINT [ "python" ]

CMD [ "server.py" ]