FROM golang:1.23-alpine AS builder

WORKDIR /app

# go.sum is optional here: if it is missing, `go mod tidy` below creates it.
COPY go.mod go.su[m] ./
RUN go mod download

COPY . .

RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o events-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/events-service .

EXPOSE 8082

CMD ["./events-service"]
