package core_test

import (
	"context"
	"sync"
	"sync-event/core"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncService_FetchSingle(t *testing.T) {
	table := []struct {
		name         string
		fromSequence uint64
		limit        uint64
		lastSequence uint64

		existed  bool
		expected []core.Event
	}{
		{
			name:         "fetch-multiples-normal",
			fromSequence: 6,
			limit:        5,

			existed: true,
			expected: []core.Event{
				{
					Sequence: 6,
					Number:   106,
				},
				{
					Sequence: 7,
					Number:   107,
				},
				{
					Sequence: 8,
					Number:   108,
				},
				{
					Sequence: 9,
					Number:   109,
				},
				{
					Sequence: 10,
					Number:   110,
				},
			},
		},
		{
			name:         "not-exist-in-buffer",
			fromSequence: 4,
			limit:        5,

			existed:  false,
			expected: nil,
		},
		{
			name:         "existed-in-buffer",
			fromSequence: 5,
			limit:        3,

			existed: true,
			expected: []core.Event{
				{
					Sequence: 5,
					Number:   105,
				},
				{
					Sequence: 6,
					Number:   106,
				},
				{
					Sequence: 7,
					Number:   107,
				},
			},
		},
		{
			name:         "lass-than-or-equal-last-sequence",
			fromSequence: 4,
			lastSequence: 4,
			limit:        5,

			existed:  false,
			expected: nil,
		},
		{
			name:         "lass-than-or-equal-last-sequence",
			fromSequence: 5,
			lastSequence: 4,
			limit:        2,

			existed: true,
			expected: []core.Event{
				{
					Sequence: 5,
					Number:   105,
				},
				{
					Sequence: 6,
					Number:   106,
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			s := core.NewSyncService(e.lastSequence,
				core.WithEventBufferSize(8),
				core.WithPublishChanSize(5),
				core.WithFetchChanSize(5),
			)

			var wg sync.WaitGroup
			wg.Add(1)

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			go func() {
				defer wg.Done()
				s.Run(ctx)
			}()

			for i := e.lastSequence + 1; i <= 12; i++ {
				s.Publish(core.Event{
					Sequence: i,
					Number:   int(100 + i),
				})
			}

			ch := make(chan core.FetchResponse, 1)

			time.Sleep(20 * time.Millisecond)

			s.Fetch(core.FetchRequest{
				FromSequence: e.fromSequence,
				Limit:        e.limit,
				ResponseChan: ch,
			})

			res := <-ch

			assert.Equal(t, e.existed, res.Existed)
			assert.Equal(t, e.expected, res.Result)

			cancel()
			wg.Wait()
		})
	}
}

func TestSyncService_FetchPanic(t *testing.T) {
	s := core.NewSyncService(0,
		core.WithEventBufferSize(8),
		core.WithPublishChanSize(5),
		core.WithFetchChanSize(5),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var r interface{}

	go func() {
		defer func() {
			r = recover()
			wg.Done()
		}()
		s.Run(ctx)
	}()

	result := make([]core.Event, 0, 5)

	ch := make(chan core.FetchResponse)

	s.Fetch(core.FetchRequest{
		FromSequence: 2,
		Limit:        5,
		Result:       result,
		ResponseChan: ch,
	})

	time.Sleep(20 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, "req.FromSequence > sequence + 1", r)
}

func TestSyncService_FetchMultiple(t *testing.T) {
	s := core.NewSyncService(0,
		core.WithEventBufferSize(8),
		core.WithPublishChanSize(5),
		core.WithFetchChanSize(5),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer wg.Done()
		s.Run(ctx)
	}()

	ch := make(chan core.FetchResponse)

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        5,
		ResponseChan: ch,
	})

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        5,
		ResponseChan: ch,
	})

	time.Sleep(20 * time.Millisecond)
	s.Publish(core.Event{
		Sequence: 1,
		Number:   101,
	})

	s.Publish(core.Event{
		Sequence: 2,
		Number:   102,
	})

	res := <-ch
	assert.True(t, res.Existed)
	expected := []core.Event{
		{
			Sequence: 1,
			Number:   101,
		},
	}
	assert.Equal(t, expected, res.Result)

	res = <-ch
	assert.True(t, res.Existed)
	expected = []core.Event{
		{
			Sequence: 1,
			Number:   101,
		},
	}
	assert.Equal(t, expected, res.Result)

	cancel()
	wg.Wait()
}

func TestSyncService_ContextCancel(t *testing.T) {
	s := core.NewSyncService(0,
		core.WithEventBufferSize(8),
		core.WithPublishChanSize(5),
		core.WithFetchChanSize(5),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer wg.Done()
		s.Run(ctx)
	}()

	ch := make(chan core.FetchResponse)

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        5,
		ResponseChan: ch,
	})

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        5,
		ResponseChan: ch,
	})

	time.Sleep(20 * time.Millisecond)

	cancel()

	var expected []core.Event

	res := <-ch
	assert.True(t, res.Existed)
	assert.Equal(t, expected, res.Result)

	res = <-ch
	assert.True(t, res.Existed)
	assert.Equal(t, expected, res.Result)

	wg.Wait()
}
