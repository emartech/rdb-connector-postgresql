FROM hseeberger/scala-sbt:8u141-jdk_2.12.3_0.13.16

ADD build.sbt /rdb-connector-postgresql/build.sbt
ADD project /rdb-connector-postgresql/project
ADD src /rdb-connector-postgresql/src

WORKDIR /rdb-connector-postgresql

RUN sbt clean compile
