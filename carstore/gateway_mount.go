package carstore

import (
	"context"
	"fmt"
	"net/url"

	"github.com/filecoin-project/dagstore/mount"
	cid "github.com/ipfs/go-cid"
)

type GatewayMount struct {
	API     GatewayAPI
	RootCID cid.Cid
}

func (g *GatewayMount) Serialize() *url.URL {
	return &url.URL{
		Host: g.RootCID.String(),
	}
}

func (g *GatewayMount) Deserialize(u *url.URL) error {
	rootCID, err := cid.Decode(u.Host)
	if err != nil {
		return fmt.Errorf("failed to parse root cid from host '%s': %w", u.Host, err)
	}
	g.RootCID = rootCID
	return nil
}

func (g *GatewayMount) Fetch(ctx context.Context) (mount.Reader, error) {
	return g.API.Fetch(ctx, g.RootCID)
}

func (g *GatewayMount) Info() mount.Info {
	return mount.Info{
		Kind:             mount.KindRemote,
		AccessSequential: true,
		AccessSeek:       false,
		AccessRandom:     false,
	}
}

func (g *GatewayMount) Close() error {
	return nil
}

func (l *GatewayMount) Stat(ctx context.Context) (mount.Stat, error) {
	// TODO: Size.
	return mount.Stat{
		Exists: true,
		Ready:  true,
	}, nil
}

func mountTemplate(api GatewayAPI) *GatewayMount {
	return &GatewayMount{API: api}
}
