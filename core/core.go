package core

import "context"

// Event ...
type Event struct {
	Sequence uint64
	Number   int
}

// FetchRequest ...
type FetchRequest struct {
	FromSequence uint64
	Limit        uint64
	Result       []Event
	ResponseChan chan FetchResponse
}

// FetchResponse ...
type FetchResponse struct {
	Existed bool
	Result  []Event
}

// SyncService ...
type SyncService struct {
	eventBufferSize uint64

	publishChan      chan Event
	fetchRequestChan chan FetchRequest
	lastSequence     uint64
}

// NewSyncService ...
func NewSyncService(lastSequence uint64, options ...SyncServiceOption) *SyncService {
	opts := syncServiceOpts{
		eventBufferSize:      5000,
		publishChanSize:      5000,
		fetchRequestChanSize: 10,
	}

	applyOptions(&opts, options...)

	return &SyncService{
		eventBufferSize:  opts.eventBufferSize,
		publishChan:      make(chan Event, opts.publishChanSize),
		fetchRequestChan: make(chan FetchRequest, opts.fetchRequestChanSize),
		lastSequence:     lastSequence,
	}
}

// Publish ...
func (s *SyncService) Publish(e Event) {
	s.publishChan <- e
}

func (s *SyncService) computeFetchResponse(req FetchRequest, events []Event, sequence uint64) FetchResponse {
	if req.FromSequence <= s.lastSequence {
		return FetchResponse{
			Existed: false,
		}
	}

	if req.FromSequence+s.eventBufferSize < sequence+1 {
		return FetchResponse{
			Existed: false,
		}
	}

	result := req.Result

	top := sequence + 1
	if top > req.FromSequence+req.Limit {
		top = req.FromSequence + req.Limit
	}

	last := top % s.eventBufferSize
	first := req.FromSequence % s.eventBufferSize

	if last >= first {
		result = append(result, events[first:last]...)
	} else {
		result = append(result, events[first:]...)
		result = append(result, events[:last]...)
	}

	return FetchResponse{
		Existed: true,
		Result:  result,
	}
}

func (s *SyncService) wait(ctx context.Context, events []Event, sequence uint64) uint64 {
	select {
	case <-ctx.Done():
		return sequence

	case event := <-s.publishChan:
		index := event.Sequence % s.eventBufferSize
		events[index] = event
		return event.Sequence

	case req := <-s.fetchRequestChan:
		if req.FromSequence > sequence+1 {
			panic("req.FromSequence > sequence + 1")
		}

		if req.FromSequence == sequence+1 {
			sequence = s.wait(ctx, events, sequence)
		}

		res := s.computeFetchResponse(req, events, sequence)
		req.ResponseChan <- res

		return sequence
	}
}

// Run ...
func (s *SyncService) Run(ctx context.Context) {
	events := make([]Event, s.eventBufferSize)
	sequence := s.lastSequence

	for {
		sequence = s.wait(ctx, events, sequence)
		if ctx.Err() != nil {
			return
		}
	}
}

// Fetch ...
func (s *SyncService) Fetch(req FetchRequest) {
	s.fetchRequestChan <- req
}
