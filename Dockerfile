FROM quay.io/prometheus/golang-builder AS builder

ADD .   /go/src/iss.digital/mt/elastic_exporter
WORKDIR /go/src/iss.digital/mt/elastic_exporter

RUN make

FROM        quay.io/prometheus/busybox:glibc
COPY        --from=builder /go/src/iss.digital/mt/elastic_exporter/elastic_exporter  /bin/elastic_exporter

EXPOSE      9399
ENTRYPOINT  [ "/bin/elastic_exporter" ]