# Build the current checkout for a Linux tmpfs ENOSPC integration test.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/nats-server .

FROM alpine:3.21
COPY --from=build /out/nats-server /usr/local/bin/nats-server
ENTRYPOINT ["/usr/local/bin/nats-server"]
