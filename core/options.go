package core

// SyncServiceOption ...
type SyncServiceOption func(opts *syncServiceOpts)

type syncServiceOpts struct {
	eventBufferSize      uint64
	publishChanSize      int
	fetchRequestChanSize int
}

func applyOptions(opts *syncServiceOpts, options ...SyncServiceOption) {
	for _, fn := range options {
		fn(opts)
	}
}

// WithEventBufferSize ...
func WithEventBufferSize(size uint64) SyncServiceOption {
	return func(opts *syncServiceOpts) {
		opts.eventBufferSize = size
	}
}

// WithPublishChanSize ...
func WithPublishChanSize(size int) SyncServiceOption {
	return func(opts *syncServiceOpts) {
		opts.publishChanSize = size
	}
}

// WithFetchChanSize ...
func WithFetchChanSize(size int) SyncServiceOption {
	return func(opts *syncServiceOpts) {
		opts.fetchRequestChanSize = size
	}
}
