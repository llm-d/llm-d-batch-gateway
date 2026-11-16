# Test-only image for the e2e media backends (see main.go). Stdlib only, so it
# builds without the e2e module's dependencies.
FROM quay.io/projectquay/golang:1.27 AS builder
WORKDIR /src
COPY main.go .
RUN go mod init mediaecho && CGO_ENABLED=0 go build -o /mediaecho .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /mediaecho /mediaecho
EXPOSE 8000
ENTRYPOINT ["/mediaecho"]
