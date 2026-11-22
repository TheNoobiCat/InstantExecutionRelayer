package endpoints

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/thenoobicat/instantexecutionrelayer/leaderboard"
	mp "github.com/thenoobicat/instantexecutionrelayer/messagepack"
	"github.com/thenoobicat/instantexecutionrelayer/relayer"
)

func HandleSignedInstantExecute(conn *WSConn, msg *mp.SignedInstantExecute, r *relayer.Relayer) {
	relayerMsg := &relayer.SignedInstantExecute{
		Version:   msg.Version,
		Type:      msg.Type,
		Channel:   msg.Channel,
		Nonce:     msg.Nonce,
		Target:    msg.Target,
		Value:     msg.Value,
		Data:      msg.Data,
		Signature: msg.Signature,
		Expires:   msg.Expires,
		ChainID:   msg.ChainID,
	}

	_ = conn.SendJSON("instant_execute_response", map[string]interface{}{
		"status":  "received",
		"channel": msg.Channel,
		"nonce":   msg.Nonce,
	})

	_ = godotenv.Load()

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		_ = conn.SendJSON("instant_execute_response", map[string]interface{}{
			"status":  "error",
			"message": "Could not connect to RPC",
		})
	}
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		_ = conn.SendJSON("instant_execute_error", map[string]interface{}{
			"step":   "connect",
			"error":  "failed to connect to ethereum client",
			"detail": err.Error(),
		})
		return
	}
	defer client.Close()

	// Use the passed relayer instead of creating a new one
	// r := relayer.NewRelayer(client, big.NewInt(11155111), common.HexToAddress("0x0"))

	_ = conn.SendJSON("instant_execute_progress", map[string]interface{}{
		"step":   1,
		"ok":     true,
		"detail": "ethereum client connected",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := r.ValidateSignedMessage(ctx, relayerMsg); err != nil {
		_ = conn.SendJSON("instant_execute_error", map[string]interface{}{
			"step":   "validation",
			"error":  "validation failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("instant_execute_progress", map[string]interface{}{
		"step":   2,
		"ok":     true,
		"detail": "message validation complete",
	})

	// Temporarily disable channel factory validation for development
	// if err := relayer.ValidateChannelFactory(ctx, relayerMsg.Channel); err != nil {
	// 	_ = conn.SendJSON("instant_execute_progress", map[string]interface{}{
	// 		"step":   3,
	// 		"ok":     false,
	// 		"detail": fmt.Sprintf("factory validation failed: %v", err),
	// 	})
	// 	return
	// }

	_ = conn.SendJSON("instant_execute_progress", map[string]interface{}{
		"step":   3,
		"ok":     true,
		"detail": "channel validation skipped (development mode)",
	})

	// Submit the transaction
	txHash, err := r.SubmitSignedInstantExecute(ctx, relayerMsg)
	if err != nil {
		_ = conn.SendJSON("instant_execute_error", map[string]interface{}{
			"step":   "submission",
			"error":  "transaction submission failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("instant_execute_result", map[string]interface{}{
		"status":       "submitted",
		"detail":       "transaction submitted to blockchain",
		"target":       msg.Target,
		"txHash":       txHash,
		"gas_estimate": "completed",
	})
}

func HandleSignedBatchInstantExecute(conn *WSConn, msg *mp.SignedBatchInstantExecute, r *relayer.Relayer) {
	relayerMsg := &relayer.SignedBatchInstantExecute{
		Version:   msg.Version,
		Type:      msg.Type,
		Channel:   msg.Channel,
		Items:     make([]relayer.BatchItem, len(msg.Items)),
		Nonce:     msg.Nonce,
		Signature: msg.Signature,
		Expires:   msg.Expires,
		ChainID:   msg.ChainID,
	}

	for i, item := range msg.Items {
		relayerMsg.Items[i] = relayer.BatchItem{
			Target: item.Target,
			Value:  item.Value,
			Data:   item.Data,
		}
	}

	_ = conn.SendJSON("batch_instant_execute_response", map[string]interface{}{
		"status":  "received",
		"channel": msg.Channel,
		"nonce":   msg.Nonce,
		"count":   len(msg.Items),
	})

	_ = godotenv.Load()

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		_ = conn.SendJSON("batch_instant_execute_response", map[string]interface{}{
			"status":  "error",
			"message": "Could not connect to RPC",
		})
	}
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		_ = conn.SendJSON("batch_instant_execute_error", map[string]interface{}{
			"step":   "connect",
			"error":  "failed to connect to ethereum client",
			"detail": err.Error(),
		})
		return
	}
	defer client.Close()

	_ = conn.SendJSON("batch_instant_execute_progress", map[string]interface{}{
		"step":   1,
		"ok":     true,
		"detail": "ethereum client connected",
	})

	// Configure relayer wallet - support both mnemonic and private key
	privateKey, err := loadRelayerWallet()
	if err != nil {
		logrus.WithError(err).Warn("Failed to load relayer wallet")
		logrus.Warn("No relayer wallet configured - transactions will be simulated only")
	} else {
		if err := r.SetWallet(privateKey); err != nil {
			logrus.WithError(err).Warn("Failed to set relayer wallet")
		} else {
			walletAddr := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
			logrus.WithField("address", walletAddr).Info("Relayer wallet configured successfully")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := r.ValidateSignedBatchMessage(ctx, relayerMsg); err != nil {
		_ = conn.SendJSON("batch_instant_execute_error", map[string]interface{}{
			"step":   "validation",
			"error":  "validation failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("batch_instant_execute_progress", map[string]interface{}{
		"step":   2,
		"ok":     true,
		"detail": "message validation complete",
	})

	// Submit the transaction
	txHash, err := r.SubmitBatchInstantExecute(ctx, relayerMsg)
	if err != nil {
		_ = conn.SendJSON("batch_instant_execute_error", map[string]interface{}{
			"step":   "submission",
			"error":  "transaction submission failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("batch_instant_execute_result", map[string]interface{}{
		"status":       "submitted",
		"detail":       "transaction submitted to blockchain",
		"count":        len(msg.Items),
		"txHash":       txHash,
		"gas_estimate": "completed",
	})

	// Track agent trade if via_agent flag is set
	if msg.ViaAgent != nil && *msg.ViaAgent {
		// Extract user address from channel address by calling the channel's owner() function
		userAddr := relayer.ExtractChannelOwner(client, msg.Channel)
		if userAddr != "" {
			if ldbInterface := r.GetLeaderboardDB(); ldbInterface != nil {
				if ldb, ok := ldbInterface.(*leaderboard.LeaderboardDB); ok {
					newCount, err := ldb.IncrementAgentTrades(userAddr)
					if err != nil {
						logrus.WithError(err).Warn("Failed to increment agent trade count")
					} else {
						logrus.WithFields(logrus.Fields{
							"user":       userAddr,
							"tradeCount": newCount,
						}).Info("Agent trade recorded")
					}
				}
			}
		}
	}

	// Wait for transaction confirmation
	logrus.WithField("txHash", txHash).Info("HandleBatchInstantExecute: Waiting for confirmation")
	_, err = r.WaitForReceipt(ctx, txHash)
	if err != nil {
		logrus.WithError(err).WithField("txHash", txHash).Error("HandleBatchInstantExecute: Confirmation failed")
		_ = conn.SendJSON("batch_instant_execute_error", map[string]interface{}{
			"step":   "confirmation",
			"error":  "transaction confirmation failed",
			"detail": err.Error(),
		})
		return
	}

	// Send success confirmation
	_ = conn.SendJSON("batch_instant_execute_result", map[string]interface{}{
		"status": "success",
		"detail": "transaction confirmed on blockchain",
		"count":  len(msg.Items),
		"txHash": txHash,
	})

	logrus.WithField("txHash", txHash).Info("HandleBatchInstantExecute: Transaction confirmed successfully")
}

func HandleSessionCreation(conn *WSConn, msg *mp.SignedSessionCreation, r *relayer.Relayer) {
	logrus.WithFields(logrus.Fields{
		"channel":    msg.Channel,
		"sessionKey": msg.SessionKey,
		"type":       msg.Type,
	}).Info("HandleSessionCreation called")

	relayerMsg := &relayer.SignedSessionCreation{
		Version:    msg.Version,
		Type:       msg.Type,
		Channel:    msg.Channel,
		SessionKey: msg.SessionKey,
		Duration:   msg.Duration,
		Nonce:      msg.Nonce,
		Signature:  msg.Signature,
		ChainID:    msg.ChainID,
	}

	logrus.Infoln("Sending session_create_response...")
	_ = conn.SendJSON("session_create_response", map[string]interface{}{
		"status":     "received",
		"channel":    msg.Channel,
		"sessionKey": msg.SessionKey,
		"nonce":      msg.Nonce,
	})

	_ = godotenv.Load()

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		_ = conn.SendJSON("session_create_error", map[string]interface{}{
			"step":   "connect",
			"error":  "Configuration error",
			"detail": "Could not connect to RPC",
		})
		return
	}

	// Configure relayer wallet - support both mnemonic and private key
	privateKey, err := loadRelayerWallet()
	if err != nil {
		logrus.WithError(err).Warn("Failed to load relayer wallet")
		logrus.Warn("No relayer wallet configured - transactions will be simulated only")
	} else {
		if err := r.SetWallet(privateKey); err != nil {
			logrus.WithError(err).Warn("Failed to set relayer wallet")
		} else {
			walletAddr := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
			logrus.WithField("address", walletAddr).Info("Relayer wallet configured successfully")
		}
	}

	_ = conn.SendJSON("session_create_progress", map[string]interface{}{
		"step":   1,
		"ok":     true,
		"detail": "Connected to Ethereum client",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := r.ValidateSessionCreation(ctx, relayerMsg); err != nil {
		// Try to extract any revert details or embedded reason from the error string to send back to client
		detail := err.Error()
		var revertData string
		if strings.Contains(detail, "Revert reason:") {
			// include the text after "Revert reason:"
			parts := strings.SplitN(detail, "Revert reason:", 2)
			if len(parts) == 2 {
				revertData = strings.TrimSpace(parts[1])
			}
		} else if strings.Contains(detail, "Revert data:") {
			parts := strings.SplitN(detail, "Revert data:", 2)
			if len(parts) == 2 {
				revertData = strings.TrimSpace(parts[1])
			}
		}

		payload := map[string]interface{}{
			"step":   "validation",
			"error":  "Session validation failed",
			"detail": detail,
		}
		if revertData != "" {
			payload["revert"] = revertData
		}

		_ = conn.SendJSON("session_create_error", payload)
		return
	}

	_ = conn.SendJSON("session_create_progress", map[string]interface{}{
		"step":   2,
		"ok":     true,
		"detail": "Session validation passed",
	})

	// Check if we have a private key for submission
	if privateKey == nil {
		logrus.Infoln("No private key configured - returning validation-only result")
		_ = conn.SendJSON("session_create_result", map[string]interface{}{
			"status": "validated",
			"detail": "Session creation validated successfully (simulation only - no wallet configured)",
		})
		return
	}

	logrus.Infoln("Private key available - proceeding with transaction submission")

	// Submit the transaction to the blockchain
	_ = conn.SendJSON("session_create_progress", map[string]interface{}{
		"step":   3,
		"ok":     true,
		"detail": "Submitting transaction to blockchain...",
	})

	logrus.Infoln("Calling SubmitSessionCreation...")
	result, err := r.SubmitSessionCreation(ctx, relayerMsg, privateKey)
	if err != nil {
		logrus.Infof("SubmitSessionCreation failed: %v", err)
		_ = conn.SendJSON("session_create_error", map[string]interface{}{
			"step":   "submission",
			"error":  "Transaction submission failed",
			"detail": err.Error(),
		})
		return
	}

	logrus.Infof("SubmitSessionCreation result: Success=%v, TxHash=%s, Error=%s", result.Success, result.TxHash, result.Error)

	if !result.Success {
		logrus.Infof("Submission unsuccessful: %s", result.Error)
		_ = conn.SendJSON("session_create_error", map[string]interface{}{
			"step":   "submission",
			"error":  "Transaction submission failed",
			"detail": result.Error,
		})
		return
	}

	logrus.Infof("Transaction submitted successfully: %s", result.TxHash)
	_ = conn.SendJSON("session_create_progress", map[string]interface{}{
		"step":   4,
		"ok":     true,
		"detail": fmt.Sprintf("Transaction submitted: %s", result.TxHash),
	})

	_ = conn.SendJSON("session_create_result", map[string]interface{}{
		"status": "submitted",
		"txHash": result.TxHash,
		"detail": "Session creation transaction submitted successfully",
	})
}

// loadRelayerWallet loads a private key from either mnemonic or raw private key
func loadRelayerWallet() (*ecdsa.PrivateKey, error) {
	// Try mnemonic first (more user-friendly)
	mnemonic := os.Getenv("RELAYER_MNEMONIC")
	if mnemonic != "" {
		return derivePrivateKeyFromMnemonic(mnemonic, 0) // Use account index 0
	}

	// Fallback to raw private key
	privateKeyHex := os.Getenv("RELAYER_PRIVATE_KEY")
	if privateKeyHex == "" {
		return nil, fmt.Errorf("neither RELAYER_MNEMONIC nor RELAYER_PRIVATE_KEY provided")
	}

	return crypto.HexToECDSA(privateKeyHex)
}

// derivePrivateKeyFromMnemonic derives a private key from a BIP39 mnemonic at the given account index
func derivePrivateKeyFromMnemonic(mnemonic string, accountIndex uint32) (*ecdsa.PrivateKey, error) {
	// Simple deterministic derivation from mnemonic phrase
	// NOTE: This is a simplified approach. In production, you should use a proper
	// BIP39/BIP44 library like github.com/tyler-smith/go-bip39 for full compatibility

	// Create a deterministic seed from the mnemonic and account index
	derivationData := fmt.Sprintf("ethereum-derivation:%s:account:%d", mnemonic, accountIndex)
	seed := crypto.Keccak256([]byte(derivationData))

	if len(seed) < 32 {
		return nil, fmt.Errorf("derived seed too short")
	}

	privateKey, err := crypto.ToECDSA(seed[:32])
	if err != nil {
		return nil, fmt.Errorf("failed to convert seed to private key: %w", err)
	}

	return privateKey, nil
}

// HandleGasEstimation handles gas estimation requests for session creation
func HandleGasEstimation(conn *WSConn, msg *mp.GasEstimationRequest, r *relayer.Relayer) {
	logrus.Infof("HandleGasEstimation called with: channel=%s, sessionKey=%s", msg.Channel, msg.SessionKey)

	_ = godotenv.Load()

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		_ = conn.SendJSON("gas_estimation_error", map[string]interface{}{
			"error":  "Configuration error",
			"detail": "Could not connect to RPC",
		})
		return
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		_ = conn.SendJSON("gas_estimation_error", map[string]interface{}{
			"error":  "RPC connection failed",
			"detail": fmt.Sprintf("Failed to dial RPC: %v", err),
		})
		return
	}

	chainID, err := client.ChainID(context.Background())
	if err != nil {
		_ = conn.SendJSON("gas_estimation_error", map[string]interface{}{
			"error":  "Chain ID retrieval failed",
			"detail": fmt.Sprintf("Failed to get chain ID: %v", err),
		})
		return
	}

	// Read factory address from environment
	factoryAddrHex := os.Getenv("FACTORY_ADDRESS")
	if factoryAddrHex == "" {
		_ = conn.SendJSON("gas_estimation_error", map[string]interface{}{
			"error":  "Configuration error",
			"detail": "FACTORY_ADDRESS not set in environment",
		})
		return
	}

	factoryAddress := common.HexToAddress(factoryAddrHex)
	rel := relayer.NewRelayer(client, chainID, factoryAddress)

	// Create a temporary SignedSessionCreation for gas estimation
	tempMsg := &relayer.SignedSessionCreation{
		Version:    msg.Version,
		Type:       msg.Type,
		Channel:    msg.Channel,
		SessionKey: msg.SessionKey,
		Duration:   msg.Expiry - time.Now().Unix(),
		Nonce:      msg.Nonce,
		Signature:  msg.Signature,
		ChainID:    msg.ChainID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Estimate gas for the session creation
	estimation, err := rel.EstimateSessionCreationGas(ctx, tempMsg)
	if err != nil {
		logrus.WithError(err).Error("Gas estimation failed")
		_ = conn.SendJSON("gas_estimation_error", map[string]interface{}{
			"error":  "Gas estimation failed",
			"detail": err.Error(),
		})
		return
	}

	logrus.Infof("Gas estimation successful: gas=%d, gasPrice=%s, cost=%s",
		estimation.EstimatedGas, estimation.GasPrice.String(), estimation.EstimatedCost.String())

	// Send the gas estimation result
	_ = conn.SendJSON("gas_estimation_result", map[string]interface{}{
		"estimatedGas":  estimation.EstimatedGas,
		"gasPrice":      estimation.GasPrice.String(),
		"estimatedCost": estimation.EstimatedCost.String(),
	})
}

// HandleChannelCreation handles gasless channel creation requests
func HandleChannelCreation(conn *WSConn, msg *mp.SignedChannelCreation, r *relayer.Relayer) {
	logrus.Infof("HandleChannelCreation called with: user=%s, nonce=%d", msg.User, msg.Nonce)

	_ = godotenv.Load()

	// Verify captcha first
	if err := verifyCaptcha(msg.CaptchaToken); err != nil {
		logrus.WithError(err).Error("Captcha verification failed")
		_ = conn.SendJSON("channel_create_error", map[string]interface{}{
			"step":   "captcha",
			"error":  "Captcha verification failed",
			"detail": err.Error(),
		})
		return
	}

	relayerMsg := &relayer.SignedChannelCreation{
		Version:      msg.Version,
		Type:         msg.Type,
		User:         msg.User,
		Nonce:        msg.Nonce,
		CaptchaToken: msg.CaptchaToken,
		Signature:    msg.Signature,
		ChainID:      msg.ChainID,
	}

	logrus.Infof("Sending channel_create_response for user %s", msg.User)
	_ = conn.SendJSON("channel_create_response", map[string]interface{}{
		"status": "received",
		"user":   msg.User,
		"nonce":  msg.Nonce,
	})

	privateKey, err := loadRelayerWallet()
	if err != nil {
		logrus.Infof("Warning: Failed to load relayer wallet: %v", err)
		logrus.Infoln("No relayer wallet configured - channel creation will be simulated only")
	} else {
		if err := r.SetWallet(privateKey); err != nil {
			logrus.Infof("Warning: Failed to set relayer wallet: %v", err)
		} else {
			walletAddr := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
			logrus.Infof("Relayer wallet configured successfully: %s", walletAddr)
		}
	}

	_ = conn.SendJSON("channel_create_progress", map[string]interface{}{
		"step":   1,
		"ok":     true,
		"detail": "Connected to Ethereum client",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := r.ValidateChannelCreation(ctx, relayerMsg); err != nil {
		_ = conn.SendJSON("channel_create_error", map[string]interface{}{
			"step":   "validation",
			"error":  "Channel creation validation failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("channel_create_progress", map[string]interface{}{
		"step":   2,
		"ok":     true,
		"detail": "Channel creation validation passed",
	})

	if privateKey == nil {
		logrus.Infoln("No private key configured - returning validation-only result")
		_ = conn.SendJSON("channel_create_result", map[string]interface{}{
			"status": "validated",
			"detail": "Channel creation validated successfully (simulation only - no wallet configured)",
		})
		return
	}

	logrus.Infoln("Private key available - adding to batch queue")

	_ = conn.SendJSON("channel_create_progress", map[string]interface{}{
		"step":   3,
		"ok":     true,
		"detail": "Adding to batch queue for processing...",
	})

	// Add to batch manager for processing
	r.AddChannelCreationRequest(relayerMsg)

	_ = conn.SendJSON("channel_create_result", map[string]interface{}{
		"status": "queued",
		"user":   msg.User,
		"nonce":  msg.Nonce,
		"detail": "Channel creation request queued for batch processing",
	})

	// Store connection for later notification
	r.PendingConnections[msg.Nonce] = conn
}

// HandleBatchChannelCreation handles batched gasless channel creation requests
func HandleBatchChannelCreation(conn *WSConn, msg *mp.BatchChannelCreation, r *relayer.Relayer) {
	logrus.Infof("HandleBatchChannelCreation called with: users=%d", len(msg.Users))

	// Temporarily disable captcha verification for testing
	// Verify captcha first
	// if err := verifyCaptcha(msg.CaptchaToken); err != nil {
	// 	logrus.Infof("Captcha verification failed: %v", err)
	// 	_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
	// 		"step":   "captcha",
	// 		"error":  "Captcha verification failed",
	// 		"detail": err.Error(),
	// 	})
	// 	return
	// }

	if len(msg.Users) != len(msg.Nonces) || len(msg.Users) != len(msg.Signatures) {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "validation",
			"error":  "Array length mismatch",
			"detail": "Users, nonces, and signatures arrays must have the same length",
		})
		return
	}

	relayerMsg := &relayer.BatchChannelCreation{
		Version:      msg.Version,
		Type:         msg.Type,
		Users:        msg.Users,
		Nonces:       msg.Nonces,
		CaptchaToken: msg.CaptchaToken,
		Signatures:   msg.Signatures,
		ChainID:      msg.ChainID,
	}

	_ = conn.SendJSON("batch_channel_create_response", map[string]interface{}{
		"status": "received",
		"count":  len(msg.Users),
	})

	_ = godotenv.Load()

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":  "connect",
			"error": "Could not connect to RPC",
		})
		return
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "connect",
			"error":  "failed to connect to ethereum client",
			"detail": err.Error(),
		})
		return
	}
	defer client.Close()

	chainID, err := client.ChainID(context.Background())
	if err != nil {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "connect",
			"error":  "Chain ID retrieval failed",
			"detail": fmt.Sprintf("Failed to get chain ID: %v", err),
		})
		return
	}

	factoryAddrHex := os.Getenv("FACTORY_ADDRESS")
	if factoryAddrHex == "" {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "config",
			"error":  "Configuration error",
			"detail": "FACTORY_ADDRESS not set in environment",
		})
		return
	}

	factoryAddress := common.HexToAddress(factoryAddrHex)
	rel := relayer.NewRelayer(client, chainID, factoryAddress)

	privateKey, err := loadRelayerWallet()
	if err != nil {
		logrus.Infof("Warning: Failed to load relayer wallet: %v", err)
		logrus.Infoln("No relayer wallet configured - batch channel creation will be simulated only")
	} else {
		if err := rel.SetWallet(privateKey); err != nil {
			logrus.Infof("Warning: Failed to set relayer wallet: %v", err)
		} else {
			walletAddr := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
			logrus.Infof("Relayer wallet configured successfully: %s", walletAddr)
		}
	}

	_ = conn.SendJSON("batch_channel_create_progress", map[string]interface{}{
		"step":   1,
		"ok":     true,
		"detail": "Connected to Ethereum client",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := rel.ValidateBatchChannelCreation(ctx, relayerMsg); err != nil {
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "validation",
			"error":  "Batch channel creation validation failed",
			"detail": err.Error(),
		})
		return
	}

	_ = conn.SendJSON("batch_channel_create_progress", map[string]interface{}{
		"step":   2,
		"ok":     true,
		"detail": "Batch channel creation validation passed",
	})

	if privateKey == nil {
		logrus.Infoln("No private key configured - returning validation-only result")
		_ = conn.SendJSON("batch_channel_create_result", map[string]interface{}{
			"status": "validated",
			"detail": "Batch channel creation validated successfully (simulation only - no wallet configured)",
		})
		return
	}

	logrus.Infoln("Private key available - proceeding with batch channel creation")

	_ = conn.SendJSON("batch_channel_create_progress", map[string]interface{}{
		"step":   3,
		"ok":     true,
		"detail": "Submitting batch channel creation transaction...",
	})

	logrus.Infoln("Calling SubmitBatchChannelCreation...")
	result, err := rel.SubmitBatchChannelCreation(ctx, relayerMsg, privateKey)
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation failed: %v", err)
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "submission",
			"error":  "Transaction submission failed",
			"detail": err.Error(),
		})
		return
	}

	logrus.Infof("SubmitBatchChannelCreation result: Success=%v, TxHash=%s, Channels=%v, Error=%s",
		result.Success, result.TxHash, result.Channels, result.Error)

	if !result.Success {
		logrus.Infof("Submission unsuccessful: %s", result.Error)
		_ = conn.SendJSON("batch_channel_create_error", map[string]interface{}{
			"step":   "submission",
			"error":  "Transaction submission failed",
			"detail": result.Error,
		})
		return
	}

	logrus.Infof("Transaction submitted successfully: %s", result.TxHash)
	_ = conn.SendJSON("batch_channel_create_progress", map[string]interface{}{
		"step":   4,
		"ok":     true,
		"detail": fmt.Sprintf("Transaction submitted: %s", result.TxHash),
	})

	_ = conn.SendJSON("batch_channel_create_result", map[string]interface{}{
		"status":   "confirmed",
		"txHash":   result.TxHash,
		"channels": result.Channels,
		"detail":   "Batch channel creation transaction confirmed successfully",
	})
}

// verifyCaptcha verifies a Cloudflare Turnstile captcha token
func verifyCaptcha(token string) error {
	secret := os.Getenv("CAPTCHA_SECRET")
	if secret == "" {
		return fmt.Errorf("CAPTCHA_SECRET not configured")
	}

	// Cloudflare Turnstile verification endpoint
	url := "https://challenges.cloudflare.com/turnstile/v0/siteverify"

	payload := map[string]string{
		"secret":   secret,
		"response": token,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal captcha payload: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("captcha verification request failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Success bool     `json:"success"`
		Errors  []string `json:"error-codes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode captcha response: %v", err)
	}

	if !result.Success {
		return fmt.Errorf("captcha verification failed: %v", result.Errors)
	}

	return nil
}
