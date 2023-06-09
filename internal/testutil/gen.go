package testutil

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var seedSeq int64

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	data := make([]byte, n)
	src := rand.NewSource(seedSeq)
	seedSeq++
	r := rand.New(src)
	_, _ = r.Read(data)
	return data
}

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(fmt.Sprint(peerSeq))
		peerIds = append(peerIds, p)
	}
	return peerIds
}

// ContainsPeer returns true if a peer is found n a list of peers.
func ContainsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}

// AssertContainsPeer will fail a test if the peer is not in the given peer list
func AssertContainsPeer(t testing.TB, peers []peer.ID, p peer.ID) {
	t.Helper()
	require.True(t, ContainsPeer(peers, p), "given peer should be in list")
}

// RefuteContainsPeer will fail a test if the peer is in the given peer list
func RefuteContainsPeer(t testing.TB, peers []peer.ID, p peer.ID) {
	t.Helper()
	require.False(t, ContainsPeer(peers, p), "given peer should not be in list")
}
