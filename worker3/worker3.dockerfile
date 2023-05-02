FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pandas
COPY worker3 /
ENTRYPOINT ["/bin/sh"]
