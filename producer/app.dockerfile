# Go Build Stage
FROM golang:1.25.0 AS buildstage

RUN mkdir /app
WORKDIR /app

COPY . .

RUN go mod download
RUN CGO_ENABLED=1 go build -o main .

CMD ["/app/main"]