FROM python:3.9.7-slim
RUN pip3 install pika
COPY rain_filter /
ENTRYPOINT ["/bin/sh"]
