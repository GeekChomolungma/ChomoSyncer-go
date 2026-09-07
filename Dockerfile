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
