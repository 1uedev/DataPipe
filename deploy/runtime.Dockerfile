# Builds the server runtime binary from the monorepo workspace root so the
# Go workspace (go.work) resolves proto/gen/go and the other local modules.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.work ./
COPY proto/gen/go ./proto/gen/go
COPY engine ./engine
COPY controlplane ./controlplane
COPY cli ./cli
COPY sdk ./sdk
ENV CGO_ENABLED=0
RUN cd engine && go build -o /out/runtime ./cmd/runtime

FROM alpine:3
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/runtime /usr/local/bin/runtime
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/runtime"]
