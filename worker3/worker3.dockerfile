FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pandas
RUN pip3 install haversine
COPY worker3 /
ENTRYPOINT ["/bin/sh"]
