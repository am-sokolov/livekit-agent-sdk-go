package main

import (
	"bytes"
	"crypto/aes"
	"fmt"
	"testing"

	"github.com/pion/rtp/codecs"
)

func TestAV1E2EEEncryptDecryptRoundTrip(t *testing.T) {
	// Minimal AV1 OBU stream with size fields (Chromium RTCEncodedVideoFrame.data format):
	// 1) OBU_TEMPORAL_DELIMITER (type 2), size=0
	// 2) OBU_SEQUENCE_HEADER (type 1), size=1, payload=[0xAA]
	// 3) OBU_FRAME_HEADER (type 3), size=2, payload=[0x00, 0xBB]
	plain := []byte{0x12, 0x00, 0x0a, 0x01, 0xaa, 0x1a, 0x02, 0x00, 0xbb}

	key := make([]byte, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	encrypted, err := encryptAV1E2EEOBUStream(plain, block, 1)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	payload, meta, ok := extractAV1E2EEMetadataOBU(encrypted)
	if !ok {
		t.Fatalf("expected AV1 metadata OBU to be present")
	}
	if got, want := len(payload), len(plain); got != want {
		t.Fatalf("payload len=%d want %d", got, want)
	}
	if got, want := meta.keyIndex, byte(1); got != want {
		t.Fatalf("keyIndex=%d want %d", got, want)
	}

	// OBU headers and size fields must remain in the clear.
	mustEq := func(i int, want byte) {
		if got := payload[i]; got != want {
			t.Fatalf("payload[%d]=0x%02x want 0x%02x", i, got, want)
		}
	}
	mustEq(0, 0x12)
	mustEq(1, 0x00)
	mustEq(2, 0x0a)
	mustEq(3, 0x01)
	mustEq(5, 0x1a)
	mustEq(6, 0x02)
	// First payload byte of OBU_FRAME_HEADER is kept clear for SFU keyframe detection.
	mustEq(7, 0x00)

	if !isAV1OBUStreamKeyframe(payload) {
		t.Fatalf("expected encrypted payload to still be detectable as keyframe")
	}

	decrypted, err := decryptAV1E2EEOBUStream(encrypted, block, nil)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, plain) {
		t.Fatalf("decrypted does not match plain: got=%x want=%x", decrypted, plain)
	}
	if !isAV1OBUStreamKeyframe(decrypted) {
		t.Fatalf("expected decrypted payload to be detectable as keyframe")
	}
}

func TestAV1LayoutCandidatesSupportAnnexB(t *testing.T) {
	plainSizeField := []byte{0x12, 0x00, 0x0a, 0x01, 0xaa, 0x1a, 0x02, 0x00, 0xbb}
	annexB, err := convertSizeFieldOBUStreamToAnnexB(plainSizeField)
	if err != nil {
		t.Fatalf("convert to annex-b: %v", err)
	}

	layouts, err := computeAV1EncryptionLayoutCandidates(annexB)
	if err != nil {
		t.Fatalf("compute layout candidates: %v", err)
	}
	if len(layouts) == 0 {
		t.Fatalf("expected at least one layout candidate")
	}

	// For this sample, the protected bytes should be:
	// - sequence header payload byte 0xAA
	// - frame header payload byte 0xBB (first payload byte 0x00 remains clear)
	expectedProtected := []byte{0xaa, 0xbb}

	foundMatchingLayout := false
	for _, layout := range layouts {
		protected := extractRanges(annexB, layout.protectedRanges, layout.protectedLength)
		if bytes.Equal(protected, expectedProtected) {
			foundMatchingLayout = true
			break
		}
	}
	if !foundMatchingLayout {
		t.Fatalf("no layout candidate produced expected protected bytes (%x)", expectedProtected)
	}
}

func TestAV1E2EEDecryptAfterRTPPacketizeDepacketize(t *testing.T) {
	// OBU stream with a larger frame payload so packetization fragments into multiple RTP packets.
	plain := []byte{
		0x12, 0x00, // temporal delimiter
		0x0a, 0x01, 0xaa, // sequence header
		0x1a, 0x44, 0x00, // frame header (first payload byte stays clear)
	}
	framePayload := make([]byte, 0x44-1)
	for i := range framePayload {
		framePayload[i] = byte((i * 31) & 0xff)
	}
	plain = append(plain, framePayload...)

	key := make([]byte, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	encrypted, err := encryptAV1E2EEOBUStream(plain, block, 1)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	payloader := &codecs.AV1Payloader{}
	encryptedPackets := payloader.Payload(32, encrypted)
	if len(encryptedPackets) < 2 {
		t.Fatalf("expected fragmented encrypted RTP payloads, got %d", len(encryptedPackets))
	}

	var encryptedDepacketizer codecs.AV1Depacketizer
	assembledEncrypted := make([]byte, 0, len(encrypted))
	for i, payload := range encryptedPackets {
		out, err := encryptedDepacketizer.Unmarshal(payload)
		if err != nil {
			t.Fatalf("depacketize encrypted packet %d: %v", i, err)
		}
		assembledEncrypted = append(assembledEncrypted, out...)
	}

	decrypted, err := decryptAV1E2EEOBUStream(assembledEncrypted, block, nil)
	if err != nil {
		t.Fatalf("decrypt after depacketize: %v", err)
	}

	plainPackets := (&codecs.AV1Payloader{}).Payload(32, plain)
	var plainDepacketizer codecs.AV1Depacketizer
	assembledPlain := make([]byte, 0, len(plain))
	for _, payload := range plainPackets {
		out, err := plainDepacketizer.Unmarshal(payload)
		if err != nil {
			t.Fatalf("depacketize plain: %v", err)
		}
		assembledPlain = append(assembledPlain, out...)
	}

	if !bytes.Equal(decrypted, assembledPlain) {
		t.Fatalf("decrypted payload after depacketization mismatch: got=%x want=%x", decrypted, assembledPlain)
	}
}

func TestAV1E2EEDecryptWithDuplicatedMetadataOBUs(t *testing.T) {
	plain := make([]byte, 0, 2048)
	plain = append(plain, 0x12, 0x00)       // temporal delimiter
	plain = append(plain, 0x0a, 0x01, 0xaa) // sequence header

	appendFrameOBU := func(payloadLen int, seed byte) {
		payload := make([]byte, payloadLen)
		payload[0] = 0x00 // keep first frame payload byte clear
		for i := 1; i < payloadLen; i++ {
			payload[i] = byte((i*37 + int(seed)) & 0xff)
		}
		plain = append(plain, byte(av1OBUTypeFrame<<3)|0x02)
		plain = append(plain, writeLeb128(uint32(payloadLen))...)
		plain = append(plain, payload...)
	}

	appendFrameOBU(180, 1)
	appendFrameOBU(220, 2)
	appendFrameOBU(260, 3)

	key := make([]byte, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	encrypted, err := encryptAV1E2EEOBUStream(plain, block, 0)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	payload, meta, ok := extractAV1E2EEMetadataOBU(encrypted)
	if !ok {
		t.Fatalf("expected AV1 metadata OBU to be present")
	}

	metaOBU, err := buildAV1E2EEMetadataOBU(meta)
	if err != nil {
		t.Fatalf("build metadata OBU: %v", err)
	}

	obus, err := splitSizeFieldOBUStream(payload)
	if err != nil {
		t.Fatalf("split OBU stream: %v", err)
	}

	// Simulate duplicated LK metadata OBUs interleaved between AV1 frame OBUs
	// (observed in the real Meet+AV1+E2EE repro before decryption).
	mutated := make([]byte, 0, len(encrypted)+2*len(metaOBU))
	inserted := 0
	for _, obu := range obus {
		mutated = append(mutated, obu...)
		obuType, _, _, ok := parseAV1OBUHeader(obu[0])
		if !ok {
			t.Fatalf("invalid OBU header in split stream")
		}
		if obuType == av1OBUTypeFrame && inserted < 2 {
			mutated = append(mutated, metaOBU...)
			inserted++
		}
	}
	mutated = append(mutated, metaOBU...)

	decrypted, err := decryptAV1E2EEOBUStream(mutated, block, nil)
	if err != nil {
		t.Fatalf("decrypt with duplicated metadata: %v", err)
	}

	if !bytes.Equal(decrypted, plain) {
		t.Fatalf("decrypted payload mismatch: got=%x want=%x", decrypted, plain)
	}
}

func TestAV1E2EEDecryptConcatenatedEncryptedChunks(t *testing.T) {
	plainA := []byte{
		0x12, 0x00,
		0x0a, 0x01, 0xaa,
		0x1a, 0x02, 0x00, 0xbb,
	}
	plainB := []byte{
		0x12, 0x00,
		0x0a, 0x01, 0xcc,
		0x1a, 0x03, 0x00, 0xdd, 0xee,
	}

	key := make([]byte, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	encA, err := encryptAV1E2EEOBUStream(plainA, block, 0)
	if err != nil {
		t.Fatalf("encrypt A: %v", err)
	}
	encB, err := encryptAV1E2EEOBUStream(plainB, block, 0)
	if err != nil {
		t.Fatalf("encrypt B: %v", err)
	}

	combinedEncrypted := append(append([]byte{}, encA...), encB...)
	decrypted, err := decryptAV1E2EEOBUStream(combinedEncrypted, block, nil)
	if err != nil {
		t.Fatalf("decrypt concatenated chunks: %v", err)
	}

	combinedPlain := append(append([]byte{}, plainA...), plainB...)
	if !bytes.Equal(decrypted, combinedPlain) {
		t.Fatalf("decrypted concatenated payload mismatch: got=%x want=%x", decrypted, combinedPlain)
	}
}

func convertSizeFieldOBUStreamToAnnexB(data []byte) ([]byte, error) {
	out := make([]byte, 0, len(data)+8)
	offset := 0
	for offset < len(data) {
		_, ext, hasSizeField, ok := parseAV1OBUHeader(data[offset])
		if !ok {
			return nil, fmt.Errorf("invalid OBU header at offset %d", offset)
		}
		headerLen := 1
		if ext {
			headerLen++
		}
		if offset+headerLen > len(data) {
			return nil, fmt.Errorf("truncated OBU header at offset %d", offset)
		}
		if !hasSizeField {
			return nil, fmt.Errorf("OBU at offset %d does not contain size field", offset)
		}

		payloadLenVal, n, ok := readLeb128(data, offset+headerLen)
		if !ok {
			return nil, fmt.Errorf("invalid leb128 at offset %d", offset+headerLen)
		}
		payloadLen := int(payloadLenVal)
		obuEnd := offset + headerLen + n + payloadLen
		if obuEnd > len(data) {
			return nil, fmt.Errorf("OBU payload extends past end at offset %d", offset)
		}

		out = append(out, writeLeb128(uint32(obuEnd-offset))...)
		out = append(out, data[offset:obuEnd]...)
		offset = obuEnd
	}
	return out, nil
}

func splitSizeFieldOBUStream(data []byte) ([][]byte, error) {
	obus := make([][]byte, 0, 8)
	offset := 0
	for offset < len(data) {
		_, ext, hasSizeField, ok := parseAV1OBUHeader(data[offset])
		if !ok {
			return nil, fmt.Errorf("invalid OBU header at offset %d", offset)
		}
		headerLen := 1
		if ext {
			headerLen++
		}
		if offset+headerLen > len(data) {
			return nil, fmt.Errorf("truncated OBU header at offset %d", offset)
		}
		if !hasSizeField {
			return nil, fmt.Errorf("OBU at offset %d does not contain size field", offset)
		}

		payloadLenVal, n, ok := readLeb128(data, offset+headerLen)
		if !ok {
			return nil, fmt.Errorf("invalid leb128 at offset %d", offset+headerLen)
		}
		payloadLen := int(payloadLenVal)
		obuEnd := offset + headerLen + n + payloadLen
		if obuEnd > len(data) {
			return nil, fmt.Errorf("OBU payload extends past end at offset %d", offset)
		}

		obus = append(obus, append([]byte(nil), data[offset:obuEnd]...))
		offset = obuEnd
	}
	return obus, nil
}
