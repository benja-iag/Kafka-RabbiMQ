FROM golang:1.19

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# RUN CGO_ENABLED=0 GOOS=linux go build -o /producer

# CMD ["/producer"]
CMD [ "go" , "run", "main.go" ]
