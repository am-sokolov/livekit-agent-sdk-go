package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"sync"

	lksdk "github.com/livekit/server-sdk-go/v2"
)

// H264 NAL unit types for RTP packet classification
const (
	// Single NAL unit types (1-23) - packet contains one complete NAL unit
	nalTypeSingleMin = 1
	nalTypeSingleMax = 23
	// STAP-A (24) - Single-Time Aggregation Packet type A
	nalTypeSTAPA = 24
	// FU-A (28) - Fragmentation Unit type A
	nalTypeFUA = 28
)

var (
	// ErrE2EENotEnabled is returned when attempting to decrypt without E2EE configuration
	ErrE2EENotEnabled = errors.New("E2EE decryption not enabled")
	// ErrSIFFrame indicates a Server Injected Frame (non-encrypted placeholder frame)
	ErrSIFFrame = errors.New("server injected frame detected")
	// ErrMalformedPayload indicates the payload is too short to contain valid encrypted data
	ErrMalformedPayload = errors.New("malformed encrypted payload")
)

// E2EEContext holds the encryption key and state for E2EE decryption.
//
// It provides thread-safe decryption of audio and video RTP payloads using
// AES-GCM 128-bit encryption. The context is initialized with a passphrase
// and a Server Injected Frame (SIF) trailer from the LiveKit room.
//
// Usage:
//
//	ctx, err := NewE2EEContext(passphrase, room.SifTrailer())
//	if err != nil {
//	    return err
//	}
//
//	// Decrypt audio payload
//	decrypted, err := ctx.DecryptAudio(rtpPayload)
//
//	// Decrypt video payload
//	decrypted, err := ctx.DecryptVideo(rtpPayload)
type E2EEContext struct {
	mu          sync.RWMutex
	key         []byte
	sifTrailer  []byte
	cipherBlock cipher.Block
	enabled     bool
}

// NewE2EEContext creates a new E2EE decryption context from a passphrase.
//
// The passphrase is used to derive a 128-bit AES key using PBKDF2 with
// the LiveKit standard salt ("LKFrameEncryptionKey"). The sifTrailer is
// used to identify Server Injected Frames (non-encrypted placeholder frames)
// which should be dropped during decryption.
//
// Parameters:
//   - passphrase: The shared secret used by all E2EE participants
//   - sifTrailer: Server Injected Frame trailer from room.SifTrailer()
//
// Returns an error if key derivation fails.
func NewE2EEContext(passphrase string, sifTrailer []byte) (*E2EEContext, error) {
	// Decode base64 encryption key to raw bytes (URL encoding without padding)
	keyBytes, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(passphrase)
	if err != nil {
		return nil, fmt.Errorf("decode encryption key: %w", err)
	}

	// Validate key size (should be 16 bytes for AES-128-GCM as per LiveKit E2EE spec)
	if len(keyBytes) != 16 {
		return nil, fmt.Errorf("invalid key size: expected 16 bytes for AES-128-GCM, got %d", len(keyBytes))
	}

	// Derive encryption key using HKDF (same as LiveKit client SDK)
	// This matches the key derivation in livekit-client's createKeyMaterialFromBuffer + deriveKeys
	// Using salt "LKFrameEncryptionKey", info 128 bytes, SHA-256 → 16-byte output
	derivedKey, err := lksdk.DeriveKeyFromBytes(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("derive encryption key: %w", err)
	}

	// Create AES cipher cipherBlock from HKDF-derived key
	// This will be used by lksdk.DecryptGCMAudioSampleCustomCipher for decryption
	cipherBlock, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	log.Printf("[e2ee] initialized E2EE context (sifTrailer len=%d)", len(sifTrailer))

	return &E2EEContext{
		key:         derivedKey,
		sifTrailer:  sifTrailer,
		cipherBlock: cipherBlock,
		enabled:     true,
	}, nil
}

// Enabled returns true if E2EE decryption is configured.
func (e *E2EEContext) Enabled() bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.enabled
}

// UpdateSifTrailer updates the Server Injected Frame trailer.
// Called when reconnecting to a room that may have a different trailer.
func (e *E2EEContext) UpdateSifTrailer(sifTrailer []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sifTrailer = sifTrailer
	log.Printf("[e2ee] updated sifTrailer (len=%d)", len(sifTrailer))
}

// DecryptAudio decrypts an E2EE-encrypted audio RTP payload.
//
// This function uses the built-in LiveKit SDK decryption function which
// handles the audio-specific encryption format (1 unencrypted byte).
//
// Returns:
//   - Decrypted payload on success
//   - nil with no error for Server Injected Frames (should be dropped)
//   - Error if decryption fails
func (e *E2EEContext) DecryptAudio(payload []byte) ([]byte, error) {
	if !e.Enabled() {
		return nil, ErrE2EENotEnabled
	}

	e.mu.RLock()
	cipherBlock := e.cipherBlock
	sifTrailer := e.sifTrailer
	e.mu.RUnlock()

	decrypted, err := lksdk.DecryptGCMAudioSampleCustomCipher(payload, sifTrailer, cipherBlock)
	if err != nil {
		return nil, fmt.Errorf("audio decryption failed: %w", err)
	}

	// nil return means this was a Server Injected Frame
	if decrypted == nil {
		return nil, nil
	}

	return decrypted, nil
}

// DecryptVideo decrypts an E2EE-encrypted video RTP payload.
//
// Video frames use the same AES-GCM encryption format as audio, but with
// different unencrypted header bytes depending on the H264 packet type:
//   - Single NAL (types 1-23): 1 byte unencrypted
//   - FU-A (type 28): 2 bytes unencrypted
//   - STAP-A (type 24): 1 byte unencrypted
//
// Encrypted payload format (same as LiveKit client SDK):
//
//	+---------+-------------------------+---------+----+
//	|frameHdr |     encrypted payload   |   IV    |len |KID|
//	+---------+-------------------------+---------+----+
//
// Where:
//   - frameHdr: Unencrypted frame header (used for AAD authentication)
//   - encrypted payload: AES-GCM encrypted video data (may need RBSP unescaping)
//   - IV: Initialization vector (typically 12 bytes)
//   - len: IV length (1 byte)
//   - KID: Key ID (1 byte, ignored - key provided externally)
//
// Returns:
//   - Decrypted payload on success
//   - nil with no error for Server Injected Frames (should be dropped)
//   - Error if decryption fails
func (e *E2EEContext) DecryptVideo(payload []byte) ([]byte, error) {
	if !e.Enabled() {
		return nil, ErrE2EENotEnabled
	}

	e.mu.RLock()
	cipherBlock := e.cipherBlock
	sifTrailer := e.sifTrailer
	e.mu.RUnlock()

	// Check for Server Injected Frame
	if sifTrailer != nil && len(payload) >= len(sifTrailer) {
		possibleTrailer := payload[len(payload)-len(sifTrailer):]
		if bytes.Equal(possibleTrailer, sifTrailer) {
			// This is an unencrypted Server Injected Frame - should be dropped
			return nil, nil
		}
	}

	// Determine unencrypted bytes based on H264 packet type
	unencryptedBytes := getH264UnencryptedBytes(payload)

	// Minimum payload size: frameHeader + ciphertext(16 byte auth tag min) + IV + trailer(2 bytes)
	minSize := unencryptedBytes + 16 + 2
	if len(payload) < minSize {
		return nil, ErrMalformedPayload
	}

	// Parse encrypted payload structure
	// Last 2 bytes: IV_LENGTH (1 byte) + KID (1 byte)
	frameTrailer := payload[len(payload)-2:]
	ivLen := int(frameTrailer[0])
	// KID := frameTrailer[1] // Key ID - ignored, we use externally provided key

	if ivLen > len(payload)-2-unencryptedBytes {
		return nil, ErrMalformedPayload
	}

	// Extract components
	frameHeader := payload[:unencryptedBytes]
	ivStart := len(payload) - 2 - ivLen
	iv := payload[ivStart : ivStart+ivLen]

	cipherTextStart := unencryptedBytes
	cipherTextEnd := ivStart
	cipherText := payload[cipherTextStart:cipherTextEnd]

	// Apply RBSP unescaping if needed for H264
	// This removes emulation prevention bytes (0x00 0x00 0x03 → 0x00 0x00)
	// that may have been inserted during encryption
	if needsRBSPUnescaping(cipherText) {
		cipherText = parseRBSP(cipherText)
	}

	// Create GCM cipher with the IV length from the payload
	aesGCM, err := cipher.NewGCMWithNonceSize(cipherBlock, ivLen)
	if err != nil {
		return nil, fmt.Errorf("create GCM cipher: %w", err)
	}

	// Decrypt using authenticated decryption
	// The frame header is used as Additional Authenticated Data (AAD)
	plainText, err := aesGCM.Open(nil, iv, cipherText, frameHeader)
	if err != nil {
		return nil, fmt.Errorf("video decryption: %w", err)
	}

	// Reconstruct the decrypted payload: frameHeader + plainText
	result := make([]byte, len(frameHeader)+len(plainText))
	copy(result[:len(frameHeader)], frameHeader)
	copy(result[len(frameHeader):], plainText)

	return result, nil
}

// IsSIFFrame checks if the payload is a Server Injected Frame.
// SIF frames are non-encrypted placeholder frames that should be dropped.
func (e *E2EEContext) IsSIFFrame(payload []byte) bool {
	if !e.Enabled() {
		return false
	}

	e.mu.RLock()
	sifTrailer := e.sifTrailer
	e.mu.RUnlock()

	if sifTrailer == nil || len(payload) < len(sifTrailer) {
		return false
	}

	possibleTrailer := payload[len(payload)-len(sifTrailer):]
	return bytes.Equal(possibleTrailer, sifTrailer)
}

// getH264UnencryptedBytes returns the number of unencrypted header bytes
// based on the H264 RTP packet type.
//
// H264 RTP packets have different structures depending on the NAL unit type:
//   - Single NAL (types 1-23): 1 byte NAL header remains unencrypted
//   - FU-A (type 28): 2 bytes (FU indicator + FU header) remain unencrypted
//   - STAP-A (type 24): 1 byte NAL header (aggregation handled specially)
//
// The unencrypted bytes serve as AAD (Additional Authenticated Data) during
// AES-GCM encryption, so the correct value must match what the encryptor used.
func getH264UnencryptedBytes(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}

	// NAL unit type is in the lower 5 bits of the first byte
	nalType := payload[0] & 0x1F

	switch {
	case nalType >= nalTypeSingleMin && nalType <= nalTypeSingleMax:
		// Single NAL unit packet: 1 byte NAL header
		return 1
	case nalType == nalTypeFUA:
		// FU-A fragmented unit: 1 byte FU indicator + 1 byte FU header
		return 2
	case nalType == nalTypeSTAPA:
		// STAP-A aggregated: 1 byte NAL header (content has variable structure)
		return 1
	default:
		// Unknown/unsupported type, use 1 byte as fallback
		return 1
	}
}

// needsRBSPUnescaping checks if the data contains emulation prevention bytes
// (0x00 0x00 0x03 sequence) that need to be removed before decryption.
//
// In H264, the byte sequence 0x00 0x00 is reserved for start codes.
// To prevent accidental start codes within NAL unit data, an emulation
// prevention byte (0x03) is inserted: 0x00 0x00 0x00 → 0x00 0x00 0x03 0x00.
// This function checks if such escaping is present.
//
// Reference: LiveKit client-sdk-js/src/e2ee/utils.ts
func needsRBSPUnescaping(data []byte) bool {
	for i := 0; i < len(data)-2; i++ {
		if data[i] == 0 && data[i+1] == 0 && data[i+2] == 3 {
			return true
		}
	}
	return false
}

// parseRBSP removes emulation prevention bytes (0x03) from the data.
//
// In H264, 0x00 0x00 0x03 sequences are used to prevent start code patterns
// within NAL units. This function reverses that escaping by removing the
// 0x03 bytes, restoring the original data.
//
// Example transformations:
//   - 0x00 0x00 0x03 0x00 → 0x00 0x00 0x00
//   - 0x00 0x00 0x03 0x01 → 0x00 0x00 0x01
//   - 0x00 0x00 0x03 0x02 → 0x00 0x00 0x02
//   - 0x00 0x00 0x03 0x03 → 0x00 0x00 0x03
//
// Reference: LiveKit client-sdk-js/src/e2ee/utils.ts
func parseRBSP(data []byte) []byte {
	result := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		if i < len(data)-2 && data[i] == 0 && data[i+1] == 0 && data[i+2] == 3 {
			// Found emulation prevention sequence, keep the two zeros but skip the 0x03
			result = append(result, data[i], data[i+1])
			i += 3 // Skip past the 0x03 emulation prevention byte
		} else {
			result = append(result, data[i])
			i++
		}
	}
	return result
}

// H264 slice NAL types (for finding unencrypted bytes boundary in frame-level decryption)
const (
	h264FrameNALTypeNonIDRSlice = 1 // Non-IDR slice (P/B frame)
	h264FrameNALTypeIDRSlice    = 5 // IDR slice (keyframe)
)

// findNALUIndices finds NALU boundaries by scanning for start codes.
// Returns indices where NAL units start (AFTER the start code, pointing to NAL header).
//
// H264 Annex B format uses start codes to delimit NAL units:
//   - 3-byte start code: 0x00 0x00 0x01
//   - 4-byte start code: 0x00 0x00 0x00 0x01
//
// This function is ported from LiveKit client-sdk-js/src/e2ee/naluUtils.ts
//
// Example:
//
//	Input:  [0x00 0x00 0x00 0x01 0x67 ... 0x00 0x00 0x01 0x68 ...]
//	Output: [4, 12]  // indices of 0x67 and 0x68 (NAL headers)
func findNALUIndices(data []byte) []int {
	var indices []int

	for i := 0; i < len(data); {
		// Check for 4-byte start code: 0x00 0x00 0x00 0x01
		if i+3 < len(data) && data[i] == 0 && data[i+1] == 0 && data[i+2] == 0 && data[i+3] == 1 {
			indices = append(indices, i+4)
			i += 4
			continue
		}

		// Check for 3-byte start code: 0x00 0x00 0x01
		if i+2 < len(data) && data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
			indices = append(indices, i+3)
			i += 3
			continue
		}

		i++
	}

	return indices
}

// isH264SliceNALU returns true if the NAL type is a slice (IDR or non-IDR).
// These are the NAL units that contain actual video data that gets encrypted.
func isH264SliceNALU(nalType byte) bool {
	return nalType == h264FrameNALTypeNonIDRSlice || nalType == h264FrameNALTypeIDRSlice
}

// findSliceNALUUnencryptedBytes finds the first slice NALU and returns the
// number of unencrypted bytes (everything up to and including first 2 bytes of slice).
//
// This matches the LiveKit client-sdk-js implementation:
//   - Find NALUs by scanning for start codes
//   - Find first slice NALU (type 1 = non-IDR, type 5 = IDR)
//   - Return index_of_slice_NALU + 2 as unencrypted bytes
//
// The unencrypted portion (AAD) includes:
//   - All start codes before the first slice
//   - All NALUs before the first slice (SPS, PPS, SEI, etc.)
//   - First 2 bytes of the slice NALU (NAL header + first slice byte)
//
// Returns 0 if no slice NALU is found (should not happen for valid video frames).
func findSliceNALUUnencryptedBytes(data []byte, naluIndices []int) int {
	for _, idx := range naluIndices {
		if idx >= len(data) {
			continue
		}
		nalType := data[idx] & 0x1F
		if isH264SliceNALU(nalType) {
			// Return index + 2 (NAL header byte + first slice data byte)
			unencrypted := idx + 2
			if unencrypted > len(data) {
				unencrypted = len(data)
			}
			return unencrypted
		}
	}
	return 0
}

// DecryptVideoFrame decrypts a complete E2EE-encrypted video frame in Annex B format.
//
// This function implements the same decryption algorithm as the LiveKit JS SDK's
// FrameCryptor.decryptFrame() method. It expects a complete video frame assembled
// from RTP packets and converted to Annex B format.
//
// Algorithm:
//  1. Find NALU indices by scanning for start codes (0x00 0x00 0x01 or 0x00 0x00 0x00 0x01)
//  2. Find first slice NALU (type 1 or 5) to determine unencryptedBytes
//  3. Extract frame header (AAD) = data[0:unencryptedBytes]
//  4. Extract encrypted data = data[unencryptedBytes:]
//  5. Apply RBSP unescaping to encrypted data if needed
//  6. Parse trailer: last 2 bytes are [ivLength][KID]
//  7. Extract IV and ciphertext
//  8. Decrypt using AES-GCM with frame header as AAD
//  9. Return reconstructed frame: header + plaintext
//
// Encrypted frame format:
//
//	[frameHeader (unencryptedBytes)][ciphertext][IV][ivLength (1 byte)][KID (1 byte)]
//
// Parameters:
//   - annexBFrame: Complete video frame in Annex B format (with start codes)
//
// Returns:
//   - Decrypted frame in Annex B format on success
//   - nil with no error for Server Injected Frames
//   - Error if decryption fails
func (e *E2EEContext) DecryptVideoFrame(annexBFrame []byte) ([]byte, error) {
	if !e.Enabled() {
		return nil, ErrE2EENotEnabled
	}

	e.mu.RLock()
	cipherBlock := e.cipherBlock
	sifTrailer := e.sifTrailer
	e.mu.RUnlock()

	// Check for Server Injected Frame
	if sifTrailer != nil && len(annexBFrame) >= len(sifTrailer) {
		possibleTrailer := annexBFrame[len(annexBFrame)-len(sifTrailer):]
		if bytes.Equal(possibleTrailer, sifTrailer) {
			return nil, nil
		}
	}

	// Step 1: Find NALU indices using start codes
	naluIndices := findNALUIndices(annexBFrame)
	if len(naluIndices) == 0 {
		return nil, fmt.Errorf("no NAL units found in frame")
	}

	// Step 2: Find first slice NALU and calculate unencrypted bytes
	unencryptedBytes := findSliceNALUUnencryptedBytes(annexBFrame, naluIndices)
	if unencryptedBytes == 0 {
		// No slice NALU found - might be SPS/PPS only frame, pass through
		return annexBFrame, nil
	}

	// Step 3: Minimum frame size check
	// frameHeader + ciphertext(16 byte auth tag min) + IV(1 min) + trailer(2 bytes)
	minSize := unencryptedBytes + 16 + 1 + 2
	if len(annexBFrame) < minSize {
		return nil, ErrMalformedPayload
	}

	// Step 4: Extract frame header (AAD)
	frameHeader := annexBFrame[:unencryptedBytes]

	// Step 5: Extract encrypted data
	encryptedData := annexBFrame[unencryptedBytes:]

	// Step 6: Apply RBSP unescaping to encrypted data if needed
	// The JS SDK applies this to the encrypted portion before decryption
	if needsRBSPUnescaping(encryptedData) {
		encryptedData = parseRBSP(encryptedData)
	}

	// Step 7: Parse trailer - last 2 bytes are [ivLength][KID]
	if len(encryptedData) < 2 {
		return nil, ErrMalformedPayload
	}
	frameTrailer := encryptedData[len(encryptedData)-2:]
	ivLength := int(frameTrailer[0])
	// KID := frameTrailer[1] // Key ID - ignored, we use externally provided key

	// Step 8: Extract IV
	if ivLength > len(encryptedData)-2 {
		return nil, ErrMalformedPayload
	}
	ivStart := len(encryptedData) - 2 - ivLength
	iv := encryptedData[ivStart : ivStart+ivLength]

	// Step 9: Extract ciphertext (everything before IV)
	ciphertext := encryptedData[:ivStart]

	// Step 10: Create GCM cipher with the IV length from the payload
	aesGCM, err := cipher.NewGCMWithNonceSize(cipherBlock, ivLength)
	if err != nil {
		return nil, fmt.Errorf("create GCM cipher: %w", err)
	}

	// Step 11: Decrypt using authenticated decryption with frame header as AAD
	plaintext, err := aesGCM.Open(nil, iv, ciphertext, frameHeader)
	if err != nil {
		return nil, fmt.Errorf("video frame decryption: %w", err)
	}

	// Step 12: Reconstruct the decrypted frame: frameHeader + plaintext
	result := make([]byte, len(frameHeader)+len(plaintext))
	copy(result[:len(frameHeader)], frameHeader)
	copy(result[len(frameHeader):], plaintext)

	return result, nil
}
