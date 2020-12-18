package publish

import (
	"context"
	"fmt"
	"sync-event/core"
	"time"
)

// PublisherID ...
type PublisherID uint32

// PublisherRepository ...
type PublisherRepository interface {
	GetLastSequence(id PublisherID) (uint64, error)
	SaveLastSequence(id PublisherID, lastSequence uint64) error
	GetEventsFromSequence(seq uint64, limit uint64) ([]core.Event, error)
}

// Publisher ...
type Publisher interface {
	Publish(events []core.Event) error
}

// PublisherService ...
type PublisherService struct {
	publisherID PublisherID
	syncService *core.SyncService
	repo        PublisherRepository
	publisher   Publisher
}

// NewPublisherService ...
func NewPublisherService(
	syncService *core.SyncService, repo PublisherRepository,
	publisherID PublisherID, p Publisher,
) *PublisherService {
	return &PublisherService{
		syncService: syncService,
		repo:        repo,
		publisherID: publisherID,
		publisher:   p,
	}
}

// Run ...
func (p *PublisherService) Run(ctx context.Context) {
	var sequence uint64
	for {
		seq, err := p.repo.GetLastSequence(p.publisherID)
		if err != nil {
			fmt.Println(err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Minute):
				continue
			}
		}
		sequence = seq
		break
	}

	ch := make(chan core.FetchResponse, 1)
	events := make([]core.Event, 0, 5000)

	for {
		p.syncService.Fetch(core.FetchRequest{
			FromSequence: sequence + 1,
			Limit:        5000,
			Result:       events,
			ResponseChan: ch,
		})

		var res core.FetchResponse
		select {
		case res = <-ch:
			break
		case <-ctx.Done():
			return
		}

		if !res.Existed {
			var err error
			res.Result, err = p.repo.GetEventsFromSequence(sequence+1, 5000)
			if err != nil {
				fmt.Println(err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Minute):
					continue
				}
			}
		}

		err := p.publisher.Publish(res.Result)
		if err != nil {
			fmt.Println(err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Minute):
				continue
			}
		}

		if len(res.Result) == 0 {
			continue
		}

		sequence = res.Result[len(res.Result)-1].Sequence

		err = p.repo.SaveLastSequence(p.publisherID, sequence)
		if err != nil {
			fmt.Println(err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Minute):
				continue
			}
		}
	}
}
