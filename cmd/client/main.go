package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/filecoin-project/saturn-l2/types"

	"github.com/google/uuid"
	cid "github.com/ipfs/go-cid"
)

var (
	L2BaseURL = flag.String("l2baseurl", "", "URL of the Saturn L2 node")
	Root      = flag.String("root", "", "root CID of the CAR to download")
)

func main() {
	flag.Parse()
	u := *L2BaseURL
	rootCid := *Root
	if u == "" || rootCid == "" {
		fmt.Fprintf(os.Stderr, "need both L2 URL and a Root Cid")
		os.Exit(2)
	}

	_, err := url.Parse(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse l2 url: %s", err.Error())
		os.Exit(2)
	}

	u = strings.TrimSuffix(u, "/")
	u = u + "/dag/car"

	c, err := cid.Decode(rootCid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse root cid: %s", err.Error())
		os.Exit(2)
	}

	body := mkRequestWithoutSelector(c)

	req, err := http.NewRequest("GET", u, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse http req: %s", err.Error())
		os.Exit(2)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send http req: %s", err.Error())
		os.Exit(2)
	}
	defer resp.Body.Close()

	fmt.Println("\n Response status code is", resp.StatusCode)

}

func mkRequestWithoutSelector(root cid.Cid) []byte {
	req := types.CARTransferRequest{
		Root:      base64.StdEncoding.EncodeToString(root.Bytes()),
		RequestId: uuid.New().String(),
	}
	reqBz, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal request to json:%s", err.Error())
		os.Exit(2)
	}
	return reqBz
}
