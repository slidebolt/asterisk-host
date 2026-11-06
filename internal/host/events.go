package host

import "context"

// waitEvents returns events after a cursor, blocking until there are some.
func (h *Host) waitEvents(ctx context.Context, streamID string, after int64) ([]Event, error) {
	for {
		h.mu.Lock()
		if streamID != "" && streamID != h.streamID {
			h.mu.Unlock()
			return nil, ErrConflict
		}
		if len(h.events) > 0 && after < h.events[0].ID-1 {
			h.mu.Unlock()
			return nil, ErrConflict
		}
		var out []Event
		for _, ev := range h.events {
			if ev.ID > after {
				out = append(out, ev)
			}
		}
		if len(out) > 0 {
			h.mu.Unlock()
			return out, nil
		}
		w := make(chan struct{})
		h.waiters = append(h.waiters, w)
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w:
		}
	}
}
