######## HADOOP DISTRIBUTION ########
# La imagen oficial solo existe para amd64. De ella se copia /opt/hadoop, que es Java y corre en cualquier arquitectura.
FROM --platform=linux/amd64 apache/hadoop:3.5.0 AS hadoop


######## BASE IMAGE ########
# La misma imagen corre todos los demonios de Hadoop y el laboratorio. Python está también en los nodos porque los
# mappers y reducers de Hadoop Streaming (lecciones 07-09) son scripts de Python que corren en los NodeManagers.
FROM python:3.13-slim-trixie AS img-base

ARG USER=hadoop
ARG USER_HOME=/home/$USER
ARG VOLUME=/app
ARG SHELL=/bin/bash

RUN pip3 install --upgrade pip

RUN apt-get update

RUN apt-get install -y locales \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen
RUN apt-get install -y sudo git curl jq less procps

# Java 17 y Hadoop 3.5: hdfs, yarn, mapred y los demonios
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=hadoop /opt/hadoop /opt/hadoop
RUN rm -rf /opt/hadoop/share/doc /opt/hadoop/etc/hadoop
ENV TZ=America/Bogota \
    JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/hadoop/conf \

    HADOOP_LOG_DIR=/data/logs \
    HADOOP_HEAPSIZE_MAX=256m \
    HDFS_JOURNALNODE_OPTS=-Xmx96m \
    HDFS_ZKFC_OPTS=-Xmx96m \
    MAPRED_HISTORYSERVER_OPTS=-Xmx192m \
    PATH=/opt/java/openjdk/bin:/opt/hadoop/bin:$PATH
RUN java -version && hadoop version | head -1

# Set up non root user (los demonios corren como "hadoop": es el superusuario de HDFS)
RUN echo "$USER ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN useradd -ms $SHELL $USER
RUN mkdir -p $VOLUME /data /hadoop-admin \
    && chown -R $USER:$USER $VOLUME /data /hadoop-admin

# Preserve bash history
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && echo $SNIPPET >> "/root/.bashrc" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R $USER /commandhistory \
    && echo $SNIPPET >> "$USER_HOME/.bashrc"

USER $USER


######## LAB IMAGE ########
FROM img-base AS img-lab

# Python requirements y dependencias de las extensiones de VS Code
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt pylint yapf isort
USER $USER
