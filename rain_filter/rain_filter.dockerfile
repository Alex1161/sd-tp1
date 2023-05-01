FROM python:3.9.7-slim
COPY rain_filter /
ENTRYPOINT ["/bin/sh"]
