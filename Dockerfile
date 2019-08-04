FROM python:3

ENV http_proxy 10.30.28.25:80
ENV https_proxy 10.30.28.25:80
ENV no_proxy 10.30.205.101
ENV PYTHONHTTPSVERIFY 0
WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install virtualenv

RUN pip install --no-cache-dir -r requirements.txt

RUN  mkdir -p /opt/oracle
RUN cd /opt/oracle

RUN wget --no-check-certificate https://github.com/adastradev/oracle-instantclient/raw/master/instantclient-basic-linux.x64-18.3.0.0.0dbru.zip

RUN ls -las
RUN unzip instantclient-basic-linux.x64-18.3.0.0.0dbru.zip -d /opt/oracle

RUN rm /usr/src/app/instantclient-basic-linux.x64-18.3.0.0.0dbru.zip

COPY libaio /opt/oracle/instantclient_18_3

RUN mv /opt/oracle/instantclient_18_3/libaio /opt/oracle/instantclient_18_3/libaio.so.1

ENV LD_LIBRARY_PATH /opt/oracle/instantclient_18_3:$LD_LIBRARY_PATH
ENV PATH $PATH:$LD_LIBRARY_PATH
COPY padfedclient.py .
COPY comparador.py .

CMD [ "python", "./padfedclient.py" ]
