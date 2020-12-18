package core_test

import (
	"sync-event/core"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncService_FetchMultiples(t *testing.T) {
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

			go func() {
				s.Run()
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
		})
	}
}
