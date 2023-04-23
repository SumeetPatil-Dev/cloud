FROM ubuntu:18.04
RUN apt update; apt install -y gnupg2
RUN apt-get -y install python3-pip
RUN pip install confluent-kafka; pip install pandas; pip install numpy; pip install numpy; pip install datetime
COPY client.properties client.properties
COPY app.py app.py
#ENTRYPOINT python3 run.py
CMD ["python3","app.py"]