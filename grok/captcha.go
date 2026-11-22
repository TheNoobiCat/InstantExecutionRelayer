package grok

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

// CaptchaVerifier handles Cloudflare Turnstile token verification.
type CaptchaVerifier struct {
	secretKey  string
	verifyURL  string
	httpClient *http.Client
}

type turnstileResponse struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes,omitempty"`
	Hostname   string   `json:"hostname,omitempty"`
}

// NewCaptchaVerifier creates a verifier with the Turnstile secret key.
func NewCaptchaVerifier(secretKey string, verifyURL string) *CaptchaVerifier {
	if verifyURL == "" {
		verifyURL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
	}
	return &CaptchaVerifier{
		secretKey: secretKey,
		verifyURL: verifyURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Verify validates a Turnstile token against the Cloudflare API.
// remoteIP is optional but recommended for additional security.
func (cv *CaptchaVerifier) Verify(token string, remoteIP string) error {
	if cv.secretKey == "" {
		return fmt.Errorf("captcha secret key not configured")
	}

	form := url.Values{
		"secret":   {cv.secretKey},
		"response": {token},
	}
	if remoteIP != "" {
		form.Set("remoteip", remoteIP)
	}

	resp, err := cv.httpClient.PostForm(cv.verifyURL, form)
	if err != nil {
		return fmt.Errorf("captcha verification request failed: %w", err)
	}
	defer resp.Body.Close()

	var result turnstileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("captcha verification decode failed: %w", err)
	}

	if !result.Success {
		logrus.WithField("errors", result.ErrorCodes).Warn("captcha verification failed")
		return fmt.Errorf("captcha verification failed: %v", result.ErrorCodes)
	}

	return nil
}
