package dremio

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
)

const (
	dremioFlightPort = "32010"
)

type ArrowFlight struct {
	FC  flight.Client
	ctx context.Context
}

func NewArrowFlight(host, id, pw string) (*ArrowFlight, error) {
	if host == "" {
		return nil, errors.New("host must not be empty")
	}
	opts := make([]grpc.DialOption, 0)
	opts = append(opts, grpc.WithInsecure())
	fc, err := flight.NewClientWithMiddleware(net.JoinHostPort(host, dremioFlightPort), nil, nil, opts...)
	if err != nil {
		return nil, err
	}
	ctx := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("routing-tag", "test-routing-tag", "routing-queue", "Low Cost User Queries"))
	if ctx, err = fc.AuthenticateBasicToken(ctx, id, pw); err != nil {
		return nil, err
	}

	return &ArrowFlight{FC: fc, ctx: ctx}, nil
}

func (af *ArrowFlight) Query(query string) (*flight.Reader, error) {
	desc := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  []byte(query),
	}

	info, err := af.FC.GetFlightInfo(af.ctx, desc)
	if err != nil {
		return nil, err
	}
	stream, err := af.FC.DoGet(af.ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, err
	}
	return rdr, nil
}
