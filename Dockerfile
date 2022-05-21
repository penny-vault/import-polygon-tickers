FROM golang:alpine AS builder
WORKDIR /go/src
RUN git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go
COPY ./ .
RUN mage -v build

FROM alpine

# Add pv as a user
RUN adduser -D pv
# Run pv as non-privileged
USER pv
WORKDIR /home/pv

COPY --from=builder /go/src/import-tickers /home/pv
ENTRYPOINT ["/home/pv/import-tickers"]
