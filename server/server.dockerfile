FROM python:3.9.7-slim
RUN pip3 install pika
COPY server /
ENTRYPOINT ["/bin/sh"]
