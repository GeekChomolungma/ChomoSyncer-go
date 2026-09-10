# Build recipe for the chomosyncer-go daemon image.
#
# Not used directly for local dev — deploy/docker-compose.yml builds it as the
# "chomosyncer-go" service (build.context = repo root, build.dockerfile = this
# file). It contains ONLY the app; Redis and ClickHouse are separate compose
# services / your own managed instances. Config is read from CHOMOSYNCER_* env
# vars (there is no config.yaml inside the image).
#
#   Build standalone:  docker build -t chomosyncer-go --build-arg VERSION=$(git describe --tags --always) .
#   Run standalone:    docker run --rm -p 9090:9090 --env-file deploy/chomosyncer-go.env chomosyncer-go

# --- build ---
FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
# CGO off => static binary that runs on distroless/static.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/chomosyncer-go ./cmd/chomosyncer-go

# --- runtime ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/chomosyncer-go /usr/local/bin/chomosyncer-go
EXPOSE 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/chomosyncer-go"]