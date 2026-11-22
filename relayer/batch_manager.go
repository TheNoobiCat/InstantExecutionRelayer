package relayer

import (
	"context"
	"crypto/ecdsa"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type BatchManager struct {
	relayer      *Relayer
	privateKey   *ecdsa.PrivateKey
	batchTimeout time.Duration
	maxBatchSize int
	currentBatch *ChannelCreationBatch
	batchMutex   sync.Mutex
	submitChan   chan *ChannelCreationBatch
	stopChan     chan struct{}
}

type ChannelCreationBatch struct {
	Requests []*SignedChannelCreation
	Timer    *time.Timer
}

func NewBatchManager(relayer *Relayer, privateKey *ecdsa.PrivateKey, batchTimeout time.Duration, maxBatchSize int) *BatchManager {
	bm := &BatchManager{
		relayer:      relayer,
		privateKey:   privateKey,
		batchTimeout: batchTimeout,
		maxBatchSize: maxBatchSize,
		submitChan:   make(chan *ChannelCreationBatch, 10),
		stopChan:     make(chan struct{}),
	}

	go bm.batchProcessor()
	return bm
}

func (bm *BatchManager) AddRequest(req *SignedChannelCreation) {
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	if bm.currentBatch == nil {
		bm.currentBatch = &ChannelCreationBatch{
			Requests: make([]*SignedChannelCreation, 0, bm.maxBatchSize),
			Timer: time.AfterFunc(bm.batchTimeout, func() {
				bm.submitCurrentBatch()
			}),
		}

	}

	bm.currentBatch.Requests = append(bm.currentBatch.Requests, req)

	if len(bm.currentBatch.Requests) >= bm.maxBatchSize {
		bm.submitCurrentBatch()
	}
}

func (bm *BatchManager) submitCurrentBatch() {
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	if bm.currentBatch == nil || len(bm.currentBatch.Requests) == 0 {
		return
	}

	// Stop the timer if it's still running
	if bm.currentBatch.Timer != nil {
		bm.currentBatch.Timer.Stop()
	}

	// Send batch for processing
	select {
	case bm.submitChan <- bm.currentBatch:
		bm.currentBatch = nil
	default:
		logrus.Printf("BatchManager: submit channel full, dropping batch of %d requests", len(bm.currentBatch.Requests))
		bm.currentBatch = nil
	}
}

func (bm *BatchManager) batchProcessor() {
	for {
		select {
		case batch := <-bm.submitChan:
			bm.processBatch(batch)
		case <-bm.stopChan:
			return
		}
	}
}

func (bm *BatchManager) processBatch(batch *ChannelCreationBatch) {
	logrus.Printf("BatchManager: Processing batch of %d channel creation requests", len(batch.Requests))

	if len(batch.Requests) == 1 {
		// Single request - use individual submission
		bm.submitSingleChannelCreation(batch.Requests[0])
	} else {
		// Multiple requests - use batch submission
		bm.submitBatchChannelCreation(batch.Requests)
	}
}

func (bm *BatchManager) submitSingleChannelCreation(req *SignedChannelCreation) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := bm.relayer.SubmitChannelCreation(ctx, req, bm.privateKey)
	if err != nil {
		logrus.Printf("BatchManager: Single channel creation failed: %v", err)
		// Notify client of failure
		if conn, exists := bm.relayer.PendingConnections[req.Nonce]; exists {
			_ = conn.SendJSON("channel_create_error", map[string]interface{}{
				"step":   "submission",
				"error":  "Channel creation failed",
				"detail": err.Error(),
			})
			delete(bm.relayer.PendingConnections, req.Nonce)
		}
		return
	}

	logrus.Printf("BatchManager: Single channel creation successful: %s -> %s", req.User, result.Channel)

	// Notify client of success
	if conn, exists := bm.relayer.PendingConnections[req.Nonce]; exists {
		_ = conn.SendJSON("channel_create_result", map[string]interface{}{
			"status":  "confirmed",
			"channel": result.Channel,
			"detail":  "Channel creation transaction confirmed successfully",
		})
		delete(bm.relayer.PendingConnections, req.Nonce)
	}
}

func (bm *BatchManager) submitBatchChannelCreation(requests []*SignedChannelCreation) {
	// Convert to batch message
	users := make([]string, len(requests))
	nonces := make([]uint64, len(requests))
	signatures := make([][]byte, len(requests))

	for i, req := range requests {
		users[i] = req.User
		nonces[i] = req.Nonce
		signatures[i] = req.Signature
	}

	batchMsg := &BatchChannelCreation{
		Version:    "1.0",
		Type:       "batch_channel_create",
		Users:      users,
		Nonces:     nonces,
		Signatures: signatures,
		ChainID:    bm.relayer.chainID.Uint64(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := bm.relayer.SubmitBatchChannelCreation(ctx, batchMsg, bm.privateKey)
	if err != nil {
		logrus.Printf("BatchManager: Batch channel creation failed: %v", err)
		// Notify all clients in the batch of failure
		for _, req := range requests {
			if conn, exists := bm.relayer.PendingConnections[req.Nonce]; exists {
				_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
					"step":   "submission",
					"error":  "Batch channel creation failed",
					"detail": err.Error(),
				})
				delete(bm.relayer.PendingConnections, req.Nonce)
			}
		}
		return
	}

	logrus.Printf("BatchManager: Batch channel creation successful: %d channels created", len(result.Channels))

	// Notify each client of success with their specific channel
	for i, req := range requests {
		if conn, exists := bm.relayer.PendingConnections[req.Nonce]; exists {
			channel := ""
			if i < len(result.Channels) {
				channel = result.Channels[i]
			}
			_ = conn.SendJSON("channel_create_result", map[string]interface{}{
				"status":  "confirmed",
				"channel": channel,
				"detail":  "Channel creation transaction confirmed successfully",
			})
			delete(bm.relayer.PendingConnections, req.Nonce)
		}
	}
}
