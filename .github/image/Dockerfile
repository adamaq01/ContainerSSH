FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go run ./cmd/license-report/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -buildvcs=false -ldflags "-s -w" -o /out/containerssh ./cmd/containerssh

FROM alpine
RUN apk upgrade --no-cache && apk add --no-cache 'libssl3' 'libcrypto3'
COPY --from=build /out/containerssh /
COPY --from=build /src/LICENSE /src/NOTICE.md /
ENTRYPOINT ["/containerssh"]
CMD ["--config", "/etc/containerssh/config.yaml"]
VOLUME /etc/containerssh
VOLUME /var/secrets
USER 1022:1022
EXPOSE 2222
EXPOSE 9100
