package main

import (
	"context"
	"fmt"
	"sync"
	"sync-event/core"
	"time"
)

func main() {
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

	res := <-ch
	fmt.Println(res)
	res = <-ch
	fmt.Println(res)

	wg.Wait()
}
