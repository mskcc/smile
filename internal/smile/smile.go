package smile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	nm "github.com/mskcc/nats-messaging-go"
	"log"
	"strconv"
	"sync"
)

const (
	requestBufSize = 1
	sampleBufSize  = 1
)

type Repository interface {
	AddRequest(context.Context, Request) error
	UpdateRequest(context.Context, []Request) error
	UpdateSample(context.Context, []Sample) error
}

type SmileArgs struct {
	URL                 string
	CertPath            string
	KeyPath             string
	Consumer            string
	Password            string
	Subject             string
	NewRequestFilter    string
	UpdateRequestFilter string
	UpdateSampleFilter  string
}

type SmileService struct {
	sa   SmileArgs
	nm   *nm.Messaging
	repo Repository
}

func NewService(sa SmileArgs, repo Repository) (*SmileService, error) {
	if repo == nil {
		return nil, errors.New("Repository must not be nil")
	}
	m, err := nm.NewSecureMessaging(sa.URL, sa.CertPath, sa.KeyPath, sa.Consumer, sa.Password)
	if err != nil {
		return nil, fmt.Errorf("cannot create a messaging connection: %w", err)
	}
	return &SmileService{sa: sa, nm: m, repo: repo}, nil
}

type requests struct {
	requests []Request
	msg      *nm.Msg
}

type samples struct {
	samples []Sample
	msg     *nm.Msg
}

func (svc *SmileService) SubscribeSmileConsumer(newRequestCh chan requests, upRequestCh chan requests, upSampleCh chan samples) error {
	err := svc.nm.Subscribe(svc.sa.Consumer, svc.sa.Subject, func(m *nm.Msg) {
		switch {
		case m.Subject == svc.sa.NewRequestFilter:
			var r Request
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err = json.Unmarshal([]byte(su), &r)
				if err != nil {
					log.Println("Error unmarshaling Request: ", err)
				} else {
					reqs := []Request{r}
					ra := requests{reqs, m}
					newRequestCh <- ra
				}
			}
			break
		case m.Subject == svc.sa.UpdateRequestFilter:
			var r []Request
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err := json.Unmarshal([]byte(su), &r)
				if err != nil {
					log.Println("Error unmarshaling Request: ", err)
				} else {
					ra := requests{r, m}
					upRequestCh <- ra
				}
			}
			break
		case m.Subject == svc.sa.UpdateSampleFilter:
			var s []Sample
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err := json.Unmarshal([]byte(su), &s)
				if err != nil {
					log.Println("Error unmarshaling Sample: ", err)
				} else {
					sa := samples{s, m}
					upSampleCh <- sa
				}
			}
			break
		default:
			// not interested in message, Ack it so we don't get it again
			m.ProviderMsg.Ack()
		}
	})
	return err
}

func (svc *SmileService) ackRequest(r requests) {
	r.msg.ProviderMsg.Ack()
}

func (svc *SmileService) ackSample(s samples) {
	s.msg.ProviderMsg.Ack()
}

func (svc *SmileService) Run(ctx context.Context) error {

	log.Println("Starting up SMILE consumer...")
	newRequestCh := make(chan requests, requestBufSize)
	updateRequestCh := make(chan requests, requestBufSize)
	updateSampleCh := make(chan samples, sampleBufSize)
	err := svc.SubscribeSmileConsumer(newRequestCh, updateRequestCh, updateSampleCh)
	if err != nil {
		return err
	}
	log.Println("SMILE consumer running...")

	var nrwg sync.WaitGroup
	var urwg sync.WaitGroup
	var uswg sync.WaitGroup
	for {
		select {
		case ra := <-newRequestCh:
			nrwg.Add(1)
			log.Printf("Processing add request: %s\n", ra.requests[0].IgoRequestID)
			go func() {
				defer nrwg.Done()
				err := svc.repo.AddRequest(ctx, ra.requests[0])
				if err != nil {
					log.Println("Error adding request: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.ackRequest(ra)
			}()
			log.Println("Processing add request complete")
		case ra := <-updateRequestCh:
			urwg.Add(1)
			log.Printf("Processing update request: %s\n", ra.requests[0].IgoRequestID)
			go func() {
				defer urwg.Done()
				err := svc.repo.UpdateRequest(ctx, ra.requests)
				if err != nil {
					log.Println("Error updating request: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.ackRequest(ra)
			}()
			log.Println("Processing update request complete")
		case sa := <-updateSampleCh:
			uswg.Add(1)
			log.Printf("Processing update sample: %s\n", sa.samples[0].CmoSampleName)
			go func() {
				defer uswg.Done()
				err := svc.repo.UpdateSample(ctx, sa.samples)
				if err != nil {
					log.Println("Error updating sample: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.ackSample(sa)
			}()
			log.Println("Processing update sample complete")
		case <-ctx.Done():
			log.Println("Context canceled, returning...")
			// tbd: check for messages being processed
			nrwg.Wait()
			urwg.Wait()
			uswg.Wait()
			svc.nm.Shutdown()
			return nil
		}
	}
}
