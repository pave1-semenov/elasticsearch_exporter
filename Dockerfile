FROM quay.io/prometheus/golang-builder AS builder

# Get sql_exporter
ADD .   /go/src/iss.digital/mt/elastic_exporter
WORKDIR /go/src/iss.digital/mt/elastic_exporter

# Do makefile
RUN make

# Make image and copy build sql_exporter
FROM        quay.io/prometheus/busybox:glibc
MAINTAINER  The Prometheus Authors <prometheus-developers@googlegroups.com>
COPY        --from=builder /go/src/iss.digital/mt/elastic_exporter/sql_exporter  /bin/sql_exporter

EXPOSE      9399
ENTRYPOINT  [ "/bin/sql_exporter" ]
