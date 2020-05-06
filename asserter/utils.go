package asserter

import (
	"encoding/json"
	"fmt"
)

// JSONObject returns an error if the provided interface cannot be unmarshaled
// into a JSON object. This is used to ensure metadata is a JSON object.
func JSONObject(obj json.RawMessage) error {
	if obj == nil {
		return nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal(obj, &m); err != nil {
		return fmt.Errorf("%w: interface is not a valid JSON object %+v", err, obj)
	}

	return nil
}
