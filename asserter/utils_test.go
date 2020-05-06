package asserter

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONObject(t *testing.T) {
	var tests = map[string]struct {
		obj json.RawMessage
		err bool
	}{
		"nil": {
			obj: nil,
			err: false,
		},
		"json array": {
			obj: json.RawMessage(`["hello"]`),
			err: true,
		},
		"json number": {
			obj: json.RawMessage(`5`),
			err: true,
		},
		"json string": {
			obj: json.RawMessage(`"hello"`),
			err: true,
		},
		"json object": {
			obj: json.RawMessage(`{"hello":"cool", "bye":[1,2,3]}`),
			err: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if test.err {
				assert.Error(t, JSONObject(test.obj))
			} else {
				assert.NoError(t, JSONObject(test.obj))
			}
		})
	}
}
