package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestL2IdIsPersisted(t *testing.T) {
	dir := t.TempDir()
	id, err := readL2IdIfExists(dir)
	require.Error(t, err)
	require.EqualValues(t, uuid.UUID{}, id)

	id, err = createAndPersistL2Id(dir)
	require.NoError(t, err)
	require.NotEqualValues(t, uuid.UUID{}, id)

	id2, err := readL2IdIfExists(dir)
	require.NoError(t, err)
	require.EqualValues(t, id, id2)
}
