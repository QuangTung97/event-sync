package main

import (
	"context"
	"fmt"
	"sync-event/core"
	"time"
)

func main() {
	s := core.NewSyncService(0,
		core.WithEventBufferSize(10),
		core.WithFetchChanSize(20),
	)

	ctx := context.Background()
	go func() {
		s.Run(ctx)
	}()

	ch := make(chan core.FetchResponse, 1)

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        4,
		Result:       make([]core.Event, 0, 5),
		ResponseChan: ch,
	})

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		s.Publish(core.Event{
			Sequence: uint64(i + 1),
			Number:   i + 100,
		})
	}

	time.Sleep(100 * time.Millisecond)

	s.Fetch(core.FetchRequest{
		FromSequence: 1,
		Limit:        4,
		Result:       make([]core.Event, 0, 5),
		ResponseChan: ch,
	})

	time.Sleep(100 * time.Millisecond)

	for i := 5; i < 10; i++ {
		s.Publish(core.Event{
			Sequence: uint64(i + 1),
			Number:   i + 100,
		})
	}

	r := <-ch
	fmt.Println("Recv:", r)

	r = <-ch
	fmt.Println("Recv:", r)

	time.Sleep(1 * time.Second)
}
