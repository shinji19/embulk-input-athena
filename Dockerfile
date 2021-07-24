FROM openjdk:8-jdk

ENV EMBULK_VERSION 0.9.23

RUN curl --create-dirs -o /usr/local/bin/embulk -L "https://dl.embulk.org/embulk-${EMBULK_VERSION}.jar" &&\
    chmod +x /usr/local/bin/embulk
