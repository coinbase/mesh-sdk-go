package asserter

import (
	"github.com/coinbase/rosetta-sdk-go/types"
)

// BlockEvent ensures a *types.BlockEvent
// is valid.
func BlockEvent(
	event *types.BlockEvent,
) error {
	if event.Sequence < 0 {
		return ErrSequenceInvalid
	}

	if err := BlockIdentifier(event.BlockIdentifier); err != nil {
		return err
	}

	switch event.Type {
	case types.ADDED, types.REMOVED:
	default:
		return ErrBlockEventTypeInvalid
	}

	return nil
}

// EventsBlocksResponse ensures a *types.EventsBlocksResponse
// is valid.
func EventsBlocksResponse(
	response *types.EventsBlocksResponse,
) error {
	if response.MaxSequence < 0 {
		return ErrMaxSequenceInvalid
	}

	for _, event := range response.Events {
		if err := BlockEvent(event); err != nil {
			return err
		}
	}

	return nil
}
