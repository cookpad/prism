FROM public.ecr.aws/amazoncorretto/amazoncorretto:11 as hadoop-libs
ARG HADOOP_VERSION=3.0.3

RUN yum install -y tar gzip

WORKDIR /tmp
# TODO: Use dlcdn.apache.org after upgrading to recent versions
RUN curl -sSfO "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz{,.sha256,.asc}"
RUN curl -sSf https://downloads.apache.org/hadoop/common/KEYS | gpg --import
RUN sha256sum -c hadoop-${HADOOP_VERSION}.tar.gz.sha256
RUN gpg --verify hadoop-${HADOOP_VERSION}.tar.gz.asc
RUN tar xf hadoop-${HADOOP_VERSION}.tar.gz && mv hadoop-${HADOOP_VERSION} hadoop

FROM public.ecr.aws/amazoncorretto/amazoncorretto:11

RUN yum install -y snappy && yum clean all && rm -rf /var/cache/yum
COPY --from=hadoop-libs /tmp/hadoop/lib/native/lib*.so /usr/lib64/
