FROM openjdk:8-jdk

# ENV EMBULK_VERSION 0.9.3
ENV EMBULK_VERSION 0.8.39

RUN curl --create-dirs -o ~/.embulk/bin/embulk -L "https://dl.bintray.com/embulk/maven/embulk-${EMBULK_VERSION}.jar" &&\
    chmod +x ~/.embulk/bin/embulk &&\
    echo 'export PATH="$HOME/.embulk/bin:$PATH"' >> ~/.bashrc
