FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023 AS hadoop-libs
ARG HADOOP_VERSION=3.5.0

RUN dnf install -y tar gzip

WORKDIR /tmp
RUN curl -sSfLO "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz{,.sha512,.asc}"
RUN curl -sSf https://downloads.apache.org/hadoop/common/KEYS | gpg --batch --no-tty --no-autostart --import
RUN sha512sum -c hadoop-${HADOOP_VERSION}.tar.gz.sha512
RUN gpg --batch --no-tty --no-autostart --verify hadoop-${HADOOP_VERSION}.tar.gz.asc hadoop-${HADOOP_VERSION}.tar.gz
RUN tar xf hadoop-${HADOOP_VERSION}.tar.gz && mv hadoop-${HADOOP_VERSION} hadoop

FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023 AS libisal
ARG LIBISAL_VERSION=2.30.0
ARG LIBISAL_SHA512SUM=d3ecfb7326097534b06a74b584100336509525ae7cadc6112d0c27e3d8704f3810e18f583d3cc33fa266bfec96db023607622b22ddbf17988ec4bf1bb3b3b9b2

RUN dnf install -y tar gzip autoconf automake libtool make nasm

WORKDIR /tmp
RUN curl -sSfLO https://github.com/intel/isa-l/archive/refs/tags/v${LIBISAL_VERSION}.tar.gz
RUN echo ${LIBISAL_SHA512SUM} v${LIBISAL_VERSION}.tar.gz | sha512sum -c -
RUN tar xf v${LIBISAL_VERSION}.tar.gz
WORKDIR /tmp/isa-l-${LIBISAL_VERSION}
RUN ./autogen.sh && ./configure --prefix=/usr --libdir=/usr/lib64 && make install

FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023

ARG MIN_CORRETTO_VERSION=21.0.13.1.2
RUN . "${JAVA_HOME}/release" && v=${IMPLEMENTOR_VERSION#Corretto-} && \
    echo "Corretto ${v}" && \
    [ "$(printf '%s\n' "${MIN_CORRETTO_VERSION}" "${v}" | sort -V | head -n1)" = "${MIN_CORRETTO_VERSION}" ] || \
    { echo "Corretto ${v} is older than ${MIN_CORRETTO_VERSION}" >&2; exit 1; }

COPY --from=hadoop-libs /tmp/hadoop/lib/native/lib*.so /usr/lib64/
COPY --from=libisal /usr/lib64/libisal.so.2 /usr/lib64/
