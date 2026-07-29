# Build the archive-relay binary.
# Multi-stage: small final image, no toolchain in the runtime layer.
FROM golang:1.25 AS build
WORKDIR /src
# cache deps
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/archive-relay ./cmd/archive-relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/archive-relay /archive-relay
# config + control.db default to /data
WORKDIR /data
USER nonroot:nonroot
EXPOSE 3334
ENTRYPOINT ["/archive-relay"]
