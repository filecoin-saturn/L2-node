package types

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var root = "QmfMYyn8LUWEfRXfijKFjBAshSsPVRUgwLZzsD7kcTtX1A"

func TestCarTransferRequest(t *testing.T) {
	c, err := cid.Decode(root)
	require.NoError(t, err)

	tcs := map[string]struct {
		cr      CARTransferRequest
		isError bool
	}{
		"invalid cid": {
			cr: CARTransferRequest{
				RequestId: uuid.New().String(),
				Root:      "test",
			},
			isError: true,
		},
		"invalid uuid": {
			cr: CARTransferRequest{
				RequestId: "blah",
				Root:      c.String(),
			},
			isError: true,
		},
		"valid request": {
			cr: CARTransferRequest{
				Root:      c.String(),
				RequestId: uuid.New().String(),
			},
			isError: false,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			bz, err := json.Marshal(tc.cr)
			require.NoError(t, err)

			var cr CARTransferRequest
			require.NoError(t, json.Unmarshal(bz, &cr))
			require.EqualValues(t, cr.Root, tc.cr.Root)

			dr, err := tc.cr.ToDAGRequest()
			if tc.isError {
				require.Error(t, err)
				require.Nil(t, dr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dr)
				require.EqualValues(t, tc.cr.RequestId, dr.RequestId.String())
				require.EqualValues(t, tc.cr.Root, dr.Root.String())
			}
		})
	}
}
