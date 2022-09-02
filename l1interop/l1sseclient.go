package l1interop

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/saturn-l2/carstore"

	"go.uber.org/atomic"

	"github.com/filecoin-project/saturn-l2/logs"

	"github.com/filecoin-project/saturn-l2/types"

	logging "github.com/ipfs/go-log/v2"

	"github.com/jpillora/backoff"
)

var (
	// 5s, 7s, 11s, 16s, 25s, 38s, 1m, 1m30s, 2m, 3m, 7m, 10m, 10m, 10m, 10m
	minBackOff           = 5 * time.Second
	maxBackOff           = 10 * time.Minute
	factor               = 1.5
	maxReconnectAttempts = 15
	maxPostResponseSize  = int64(102400) // 100 Kib

	log = logging.Logger("l1-interop")
)

var (
	l1RegisterURL = "https://%s/register/%s"
	l1PostURL     = "https://%s/data/%s?requestId=%s"
)

type l1SseClient struct {
	ctx     context.Context
	cancelF context.CancelFunc

	l1Addr string

	client *http.Client
	l2Id   string

	minBackOffWait       time.Duration
	maxBackoffWait       time.Duration
	backOffFactor        float64
	maxReconnectAttempts float64

	cs     carServer
	logger *logs.SaturnLogger

	wg sync.WaitGroup

	semaphore chan struct{}
}

type carServer interface {
	ServeCARFile(ctx context.Context, dr *types.DagTraversalRequest, w io.Writer) error
}

func New(l2Id string, client *http.Client, logger *logs.SaturnLogger, cs carServer, l1Addr string, maxConcurrentReqs int) *l1SseClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &l1SseClient{
		ctx:                  ctx,
		cancelF:              cancel,
		client:               client,
		l2Id:                 l2Id,
		minBackOffWait:       minBackOff,
		maxBackoffWait:       maxBackOff,
		backOffFactor:        factor,
		maxReconnectAttempts: float64(maxReconnectAttempts),
		logger:               logger,
		cs:                   cs,
		l1Addr:               l1Addr,
		semaphore:            make(chan struct{}, maxConcurrentReqs),
	}
}

func (l *l1SseClient) Start(nConnectedl1s *atomic.Uint64) error {
	backoff := &backoff.Backoff{
		Min:    l.minBackOffWait,
		Max:    l.maxBackoffWait,
		Factor: factor,
		Jitter: true,
	}

	l1url := fmt.Sprintf(l1RegisterURL, l.l1Addr, l.l2Id)

	var resp *http.Response

	for {
		if resp != nil && resp.Body != nil {
			lr := io.LimitReader(resp.Body, maxPostResponseSize)
			_, _ = io.Copy(io.Discard, lr)
			resp.Body.Close()
		}
		// if context has already been cancelled, return immediately
		if l.ctx.Err() != nil {
			return l.ctx.Err()
		}

		// construct an http register request to send to the L1 with the given context
		req, err := http.NewRequest("GET", l1url, nil)
		if err != nil {
			return fmt.Errorf("failed to create http req: %w", err)
		}
		req = req.WithContext(l.ctx)

		doBackOffFn := func() error {
			// if we've exhausted the maximum number of connection attempts with the L1, return.
			if backoff.Attempt() > l.maxReconnectAttempts {
				log.Errorw("failed to connect to l1; exhausted max attempts", "l1", l.l1Addr, "err", err)
				return fmt.Errorf("failed to connect to l1 after exhausting max attempts, l1: %s, err: %w", l.l1Addr, err)
			}

			// backoff and wait before making a new connection attempt to the L1.
			duration := backoff.Duration()
			bt := time.NewTimer(duration)
			defer bt.Stop()
			select {
			case <-bt.C:
				log.Infow("back-off complete, retrying http register request to l1",
					"backoff time", duration.String(), "l1 Addr", l.l1Addr)
			case <-l.ctx.Done():
				log.Errorw("did not retry http request: context cancelled", "err", l.ctx.Err(), "l1", l.l1Addr)
				return l.ctx.Err()
			}
			return nil
		}

		// make an http connection with keep alive
		resp, err = l.client.Do(req)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return l.ctx.Err()
			}
			log.Errorw("failed to send register request to l1; will backoff and retry", "l1", l.l1Addr, "err", err)

			if err := doBackOffFn(); err != nil {
				return err
			}
			continue
		}
		defer resp.Body.Close()

		// return immediately if we got a 4xx response status code from the L1.
		if resp.StatusCode/100 == 4 {
			log.Errorw("http registration request to L1 failed with non-retryable status code; returning",
				"status code", resp.StatusCode, "l1", l.l1Addr)
			return fmt.Errorf("terminating http request: received %d response from L1", resp.StatusCode)
		}

		// if we got anything other than a 200 for the registration -> retry
		if resp.StatusCode != http.StatusOK {
			log.Errorw("http registration request to l1 got invalid status code; will backoff and retry registration",
				"code", resp.StatusCode, "l1", l.l1Addr)
			if err := doBackOffFn(); err != nil {
				return err
			}
			continue
		}

		// we've registered successfully -> reset the backoff counter
		backoff.Reset()
		n := nConnectedl1s.Inc()
		log.Infow("new L1 connection established", "l1", l.l1Addr, "nL1sConnected", n)

		// we've successfully connected to the L1, start reading new line delimited json requests for CAR files
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			reqJSON := scanner.Text()
			if len(reqJSON) == 0 {
				continue
			}

			log.Debugw("will try to acquire semaphore for l1 request", "l1", l.l1Addr)
			select {
			case l.semaphore <- struct{}{}:
				log.Debugw("successfully acquired semaphore for l1 request", "l1", l.l1Addr)
			case <-l.ctx.Done():
				return l.ctx.Err()
			}

			releaseSem := func() {
				select {
				case <-l.semaphore:
					log.Debugw("successfully released semaphore for l1 request", "l1", l.l1Addr)
				case <-l.ctx.Done():
					return
				}
			}

			log.Infow("received request from L1", "l1", l.l1Addr, "json", reqJSON)

			var carReq types.CARTransferRequest
			if err := json.Unmarshal([]byte(reqJSON), &carReq); err != nil {
				releaseSem()
				return fmt.Errorf("could not unmarshal l1 request: req=%s, err=%w", reqJSON, err)
			}

			dr, err := carReq.ToDAGRequest()
			if err != nil {
				releaseSem()
				return fmt.Errorf("could not parse car transfer request,err=%w", err)
			}

			l.logger.Infow(dr.RequestId, "parsed CAR transfer request received from L1", "l1", l.l1Addr, "req", dr)

			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				defer releaseSem()

				if err := l.sendCarResponse(l.ctx, l.l1Addr, dr); err != nil {
					if !errors.Is(err, carstore.ErrNotFound) {
						l.logger.Errorw(dr.RequestId, "failed to send CAR file to L1 using Post", "err", err, "l1", l.l1Addr)
					} else {
						l.logger.Infow(dr.RequestId, "not sending CAR over POST", "err", err, "l1", l.l1Addr)
					}
				}
			}()
		}

		if err := scanner.Err(); err != nil {
			log.Errorw("error while reading l1 requests; will reconnect and retry", "err", err)
		}

		n = nConnectedl1s.Dec()
		log.Infow("lost connection to L1", "l1", l.l1Addr, "nL1sConnected", n)
	}
}

func (l *l1SseClient) serve() {

}

func (l *l1SseClient) Stop() {
	l.cancelF()
	l.wg.Wait()
}

func (l *l1SseClient) sendCarResponse(ctx context.Context, l1Addr string, dr *types.DagTraversalRequest) error {
	respUrl := fmt.Sprintf(l1PostURL, l1Addr, dr.Root.String(), dr.RequestId.String())

	prd, pw := io.Pipe()
	defer prd.Close()
	defer pw.Close()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		err := l.cs.ServeCARFile(ctx, dr, pw)
		_ = pw.CloseWithError(err)
	}()

	req, err := http.NewRequest(http.MethodPost, respUrl, prd)
	if err != nil {
		_ = prd.CloseWithError(err)
		return fmt.Errorf("failed to create http post request to send back car to L1; url=%s; err=%w", respUrl, err)
	}
	req = req.WithContext(ctx)

	resp, err := l.client.Do(req)
	if err != nil {
		_ = prd.CloseWithError(err)
		return fmt.Errorf("failed to send http post request with CAR to L1;url=%s, err=%w", respUrl, err)
	}
	defer resp.Body.Close()
	_ = prd.Close()

	lr := io.LimitReader(resp.Body, maxPostResponseSize)
	_, _ = io.Copy(ioutil.Discard, lr)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got status code %d from L1 for POST url %s , expected %d", resp.StatusCode, respUrl, http.StatusOK)
	}

	l.logger.Infow(dr.RequestId, "successfully sent CAR file to L1", "l1", l1Addr, "url", respUrl)
	return nil
}
