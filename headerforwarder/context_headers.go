package headerforwarder

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const requestIDKey = contextKey("request_id")

func ContextWithRosettaID(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestIDKey, uuid.NewString())
}

func RosettaIDFromContext(ctx context.Context) string {
	return ctx.Value(requestIDKey).(string)
}

func RosettaIDFromRequest(r *http.Request) string {
	switch value := r.Context().Value(requestIDKey).(type) {
	case string:
		return value
	default:
		return ""
	}
}
