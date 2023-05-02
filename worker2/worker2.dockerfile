FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pandas
COPY worker2 /
ENTRYPOINT ["/bin/sh"]
