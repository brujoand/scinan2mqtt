FROM python:3.12-slim

RUN pip install --no-cache-dir aiomqtt==2.5.1

COPY bridge.py /bridge.py

USER 65534:65534

CMD ["python", "/bridge.py"]
