FROM golang:1.22-alpine3.21 AS builder
RUN apk --no-cache update && apk --no-cache add gcc musl-dev git make bash
WORKDIR /project
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 make bin

FROM alpine:3.21
RUN apk --no-cache update
WORKDIR /project
COPY --from=builder /project/scheduler scheduler
USER 1001
CMD ["./scheduler"]
