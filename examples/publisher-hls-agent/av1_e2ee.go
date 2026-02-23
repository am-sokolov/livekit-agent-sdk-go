package main

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"sync"
)

const (
	av1GCMTagLengthBytes = 16

	av1E2EEMetadataOBUType = 5

	av1E2EEMetadataMagic0  = 0x4c // 'L'
	av1E2EEMetadataMagic1  = 0x4b // 'K'
	av1E2EEMetadataVersion = 1

	av1OBUTypeFrameHeader = 3
	av1OBUTypeFrame       = 6
)

type av1ByteRange struct {
	start int
	end   int
}

type av1EncryptionLayout struct {
	protectedRanges []av1ByteRange
	protectedLength int
}

type av1E2EEMetadata struct {
	keyIndex byte
	iv       []byte
	tag      []byte
}

type av1E2EEEncryptedChunk struct {
	payload []byte
	meta    av1E2EEMetadata
}

var av1DecryptDebugOnce sync.Once

func isAV1OBUStreamKeyframe(data []byte) bool {
	offset := 0
	for offset < len(data) {
		obuType, ext, hasSizeField, ok := parseAV1OBUHeader(data[offset])
		if !ok {
			return false
		}
		headerLen := 1
		if ext {
			headerLen++
		}
		if offset+headerLen > len(data) {
			return false
		}

		payloadLen := len(data) - (offset + headerLen)
		sizeFieldLen := 0
		if hasSizeField {
			val, n, ok := readLeb128(data, offset+headerLen)
			if !ok {
				return false
			}
			payloadLen = int(val)
			sizeFieldLen = n
			if offset+headerLen+sizeFieldLen+payloadLen > len(data) {
				return false
			}
		}

		payloadStart := offset + headerLen + sizeFieldLen
		payloadEnd := payloadStart + payloadLen

		if (obuType == av1OBUTypeFrameHeader || obuType == av1OBUTypeFrame) && payloadLen > 0 {
			b := data[payloadStart]
			showExistingFrame := (b & 0x01) != 0
			if !showExistingFrame {
				frameType := (b >> 1) & 0x03
				if frameType == 0 {
					return true
				}
			}
		}

		offset = payloadEnd
		if !hasSizeField {
			break
		}
	}
	return false
}

func decryptAV1E2EEOBUStream(encrypted []byte, cipherBlock cipher.Block, sifTrailer []byte) ([]byte, error) {
	if cipherBlock == nil {
		return nil, fmt.Errorf("cipherBlock is required")
	}
	if len(encrypted) == 0 {
		return nil, nil
	}
	if sifTrailer != nil && len(encrypted) >= len(sifTrailer) && bytes.HasSuffix(encrypted, sifTrailer) {
		return nil, nil
	}

	chunks, err := splitAV1E2EEEncryptedChunks(encrypted)
	if err != nil {
		return nil, err
	}
	chunks = filterNonEmptyAV1E2EEChunks(chunks)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("missing AV1 E2EE payload")
	}

	if len(chunks) == 1 {
		return decryptAV1E2EEPayloadWithMeta(chunks[0].payload, chunks[0].meta, cipherBlock)
	}

	if av1AllChunkMetadataEqual(chunks) {
		mergedPayload := mergeAV1E2EEChunkPayloads(chunks)
		mergedMeta := chunks[len(chunks)-1].meta
		if decrypted, err := decryptAV1E2EEPayloadWithMeta(mergedPayload, mergedMeta, cipherBlock); err == nil {
			return decrypted, nil
		}
	}

	totalLen := 0
	for _, chunk := range chunks {
		totalLen += len(chunk.payload)
	}

	out := make([]byte, 0, totalLen)
	for i, chunk := range chunks {
		decrypted, err := decryptAV1E2EEPayloadWithMeta(chunk.payload, chunk.meta, cipherBlock)
		if err != nil {
			return nil, fmt.Errorf("decrypt chunk %d/%d: %w", i+1, len(chunks), err)
		}
		out = append(out, decrypted...)
	}
	return out, nil
}

func decryptAV1E2EEPayloadWithMeta(payload []byte, meta av1E2EEMetadata, cipherBlock cipher.Block) ([]byte, error) {
	if len(meta.iv) != 12 {
		return nil, fmt.Errorf("unexpected IV length: %d", len(meta.iv))
	}
	if len(meta.tag) != av1GCMTagLengthBytes {
		return nil, fmt.Errorf("unexpected auth tag length: %d", len(meta.tag))
	}

	layouts, err := computeAV1EncryptionLayoutCandidates(payload)
	if err != nil {
		return nil, fmt.Errorf("compute layout: %w", err)
	}

	aesGCM, err := cipher.NewGCMWithNonceSize(cipherBlock, len(meta.iv))
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	var decryptErr error
	for _, layout := range layouts {
		cipherProtected := extractRanges(payload, layout.protectedRanges, layout.protectedLength)
		if len(cipherProtected) != layout.protectedLength {
			continue
		}

		cipherProtectedWithTag := make([]byte, len(cipherProtected)+len(meta.tag))
		copy(cipherProtectedWithTag, cipherProtected)
		copy(cipherProtectedWithTag[len(cipherProtected):], meta.tag)

		plainProtected, openErr := aesGCM.Open(nil, meta.iv, cipherProtectedWithTag, nil)
		if openErr != nil {
			decryptErr = openErr
			continue
		}
		if len(plainProtected) != layout.protectedLength {
			decryptErr = fmt.Errorf("unexpected plaintext length: got %d want %d", len(plainProtected), layout.protectedLength)
			continue
		}

		out := make([]byte, len(payload))
		copy(out, payload)
		if err := writeRanges(out, layout.protectedRanges, plainProtected, layout.protectedLength); err != nil {
			decryptErr = err
			continue
		}
		return out, nil
	}

	if decryptErr == nil {
		decryptErr = fmt.Errorf("no AV1 layout candidate matched")
	}

	av1DecryptDebugOnce.Do(func() {
		payloadHash := sha256.Sum256(payload)
		log.Printf("[av1-e2ee-debug] decrypt failed: payloadLen=%d payloadSHA256=%x keyIndex=%d iv=%x tag=%x layoutCandidates=%d",
			len(payload), payloadHash, meta.keyIndex, meta.iv, meta.tag, len(layouts))

		for i, layout := range layouts {
			protected := extractRanges(payload, layout.protectedRanges, layout.protectedLength)
			protectedHash := sha256.Sum256(protected)
			log.Printf("[av1-e2ee-debug] layout[%d]=%s protectedLen=%d protectedSHA256=%x",
				i, av1LayoutSignature(layout), len(protected), protectedHash)
		}

		// Log one payload sample for offline comparison with JS worker logic.
		if len(payload) <= 16384 {
			log.Printf("[av1-e2ee-debug] payloadBase64=%s", base64.StdEncoding.EncodeToString(payload))
		}
	})

	return nil, fmt.Errorf("decrypt protected bytes: %w", decryptErr)
}

func encryptAV1E2EEOBUStream(plain []byte, cipherBlock cipher.Block, keyIndex byte) ([]byte, error) {
	if cipherBlock == nil {
		return nil, fmt.Errorf("cipherBlock is required")
	}
	if len(plain) == 0 {
		return nil, nil
	}

	layout, err := computeAV1EncryptionLayout(plain)
	if err != nil {
		return nil, fmt.Errorf("compute layout: %w", err)
	}

	plainProtected := extractRanges(plain, layout.protectedRanges, layout.protectedLength)

	iv := make([]byte, 12)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("generate IV: %w", err)
	}

	aesGCM, err := cipher.NewGCMWithNonceSize(cipherBlock, len(iv))
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	cipherProtectedWithTag := aesGCM.Seal(nil, iv, plainProtected, nil)
	if len(cipherProtectedWithTag) != len(plainProtected)+av1GCMTagLengthBytes {
		return nil, fmt.Errorf("unexpected AES-GCM output length: got %d want %d", len(cipherProtectedWithTag), len(plainProtected)+av1GCMTagLengthBytes)
	}

	cipherProtected := cipherProtectedWithTag[:len(plainProtected)]
	tag := cipherProtectedWithTag[len(plainProtected):]

	metaOBU, err := buildAV1E2EEMetadataOBU(av1E2EEMetadata{
		keyIndex: keyIndex,
		iv:       iv,
		tag:      tag,
	})
	if err != nil {
		return nil, err
	}

	out := make([]byte, len(plain), len(plain)+len(metaOBU))
	copy(out, plain)
	if err := writeRanges(out, layout.protectedRanges, cipherProtected, layout.protectedLength); err != nil {
		return nil, err
	}
	out = append(out, metaOBU...)

	return out, nil
}

func buildAV1E2EEMetadataOBU(meta av1E2EEMetadata) ([]byte, error) {
	if len(meta.iv) != 12 {
		return nil, fmt.Errorf("unexpected IV length: %d", len(meta.iv))
	}
	if len(meta.tag) != av1GCMTagLengthBytes {
		return nil, fmt.Errorf("unexpected auth tag length: %d", len(meta.tag))
	}

	payload := make([]byte, 32)
	payload[0] = av1E2EEMetadataMagic0
	payload[1] = av1E2EEMetadataMagic1
	payload[2] = av1E2EEMetadataVersion
	payload[3] = meta.keyIndex
	copy(payload[4:16], meta.iv)
	copy(payload[16:32], meta.tag)

	obuHeader := byte(av1E2EEMetadataOBUType<<3) | 0x02
	sizeField := writeLeb128(uint32(len(payload)))

	out := make([]byte, 1+len(sizeField)+len(payload))
	out[0] = obuHeader
	copy(out[1:], sizeField)
	copy(out[1+len(sizeField):], payload)
	return out, nil
}

func extractAV1E2EEMetadataOBU(data []byte) ([]byte, av1E2EEMetadata, bool) {
	chunks, err := splitAV1E2EEEncryptedChunks(data)
	if err != nil {
		return nil, av1E2EEMetadata{}, false
	}
	chunks = filterNonEmptyAV1E2EEChunks(chunks)
	if len(chunks) == 0 {
		return nil, av1E2EEMetadata{}, false
	}

	payload := mergeAV1E2EEChunkPayloads(chunks)
	meta := chunks[len(chunks)-1].meta
	return payload, meta, true
}

func splitAV1E2EEEncryptedChunks(data []byte) ([]av1E2EEEncryptedChunk, error) {
	if len(data) == 0 {
		return nil, nil
	}

	chunks := make([]av1E2EEEncryptedChunk, 0, 4)
	currentPayload := make([]byte, 0, len(data))

	offset := 0
	for offset < len(data) {
		obuStart := offset

		obuType, ext, hasSizeField, ok := parseAV1OBUHeader(data[offset])
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
			return nil, fmt.Errorf("missing OBU size field at offset %d", offset)
		}

		val, n, ok := readLeb128(data, offset+headerLen)
		if !ok {
			return nil, fmt.Errorf("invalid OBU size leb128 at offset %d", offset+headerLen)
		}
		payloadLen := int(val)
		payloadStart := offset + headerLen + n
		payloadEnd := payloadStart + payloadLen
		if payloadEnd > len(data) {
			return nil, fmt.Errorf("OBU payload extends past end at offset %d", offset)
		}
		obuEnd := payloadEnd

		if obuType == av1E2EEMetadataOBUType {
			if meta, ok := parseAV1E2EEMetadataPayload(data[payloadStart:payloadEnd]); ok {
				chunkPayload := append([]byte(nil), currentPayload...)
				chunks = append(chunks, av1E2EEEncryptedChunk{
					payload: chunkPayload,
					meta:    meta,
				})
				currentPayload = currentPayload[:0]
				offset = obuEnd
				continue
			}
		}

		currentPayload = append(currentPayload, data[obuStart:obuEnd]...)
		offset = obuEnd
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("missing AV1 E2EE metadata OBU")
	}
	if len(currentPayload) != 0 {
		return nil, fmt.Errorf("trailing AV1 payload after E2EE metadata OBU")
	}
	return chunks, nil
}

func parseAV1E2EEMetadataPayload(payload []byte) (av1E2EEMetadata, bool) {
	if len(payload) != 32 {
		return av1E2EEMetadata{}, false
	}
	if payload[0] != av1E2EEMetadataMagic0 || payload[1] != av1E2EEMetadataMagic1 || payload[2] != av1E2EEMetadataVersion {
		return av1E2EEMetadata{}, false
	}
	return av1E2EEMetadata{
		keyIndex: payload[3],
		iv:       append([]byte(nil), payload[4:16]...),
		tag:      append([]byte(nil), payload[16:32]...),
	}, true
}

func filterNonEmptyAV1E2EEChunks(chunks []av1E2EEEncryptedChunk) []av1E2EEEncryptedChunk {
	if len(chunks) == 0 {
		return nil
	}

	filtered := make([]av1E2EEEncryptedChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk.payload) == 0 {
			continue
		}
		filtered = append(filtered, chunk)
	}
	return filtered
}

func mergeAV1E2EEChunkPayloads(chunks []av1E2EEEncryptedChunk) []byte {
	totalLen := 0
	for _, chunk := range chunks {
		totalLen += len(chunk.payload)
	}

	merged := make([]byte, 0, totalLen)
	for _, chunk := range chunks {
		merged = append(merged, chunk.payload...)
	}
	return merged
}

func av1AllChunkMetadataEqual(chunks []av1E2EEEncryptedChunk) bool {
	if len(chunks) <= 1 {
		return true
	}
	first := chunks[0].meta
	for i := 1; i < len(chunks); i++ {
		if !av1E2EEMetadataEqual(first, chunks[i].meta) {
			return false
		}
	}
	return true
}

func av1E2EEMetadataEqual(a, b av1E2EEMetadata) bool {
	if a.keyIndex != b.keyIndex {
		return false
	}
	return bytes.Equal(a.iv, b.iv) && bytes.Equal(a.tag, b.tag)
}

func computeAV1EncryptionLayout(data []byte) (*av1EncryptionLayout, error) {
	layouts, err := computeAV1EncryptionLayoutCandidates(data)
	if err != nil {
		return nil, err
	}
	return layouts[0], nil
}

func computeAV1EncryptionLayoutCandidates(data []byte) ([]*av1EncryptionLayout, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty frame")
	}

	firstByte := data[0]
	looksLikeOBUHeader := (firstByte&0x80) == 0 && (firstByte&0x01) == 0
	looksLikeRTPAggregationHeader := (firstByte & 0x07) == 0

	parsers := make([]func([]byte) (*av1EncryptionLayout, error), 0, 4)
	if looksLikeOBUHeader {
		parsers = append(parsers, computeAV1LayoutFromSizeFieldOBUStream, computeAV1LayoutFromAnnexB)
		if looksLikeRTPAggregationHeader {
			parsers = append(parsers, computeAV1LayoutFromRTPPayload)
		}
		parsers = append(parsers, computeAV1LayoutFromRTXPayload)
	} else if looksLikeRTPAggregationHeader {
		parsers = append(parsers, computeAV1LayoutFromRTPPayload, computeAV1LayoutFromRTXPayload)
		parsers = append(parsers, computeAV1LayoutFromSizeFieldOBUStream, computeAV1LayoutFromAnnexB)
	} else {
		parsers = append(
			parsers,
			computeAV1LayoutFromSizeFieldOBUStream,
			computeAV1LayoutFromAnnexB,
			computeAV1LayoutFromRTPPayload,
			computeAV1LayoutFromRTXPayload,
		)
	}

	layouts := make([]*av1EncryptionLayout, 0, len(parsers))
	seen := make(map[string]struct{}, len(parsers))
	for _, parse := range parsers {
		layout, err := parse(data)
		if err != nil {
			continue
		}
		if !isValidAV1Layout(layout, len(data)) {
			continue
		}
		signature := av1LayoutSignature(layout)
		if _, exists := seen[signature]; exists {
			continue
		}
		seen[signature] = struct{}{}
		layouts = append(layouts, layout)
	}

	if len(layouts) == 0 {
		return nil, fmt.Errorf("layout detection failed")
	}
	return layouts, nil
}

func computeAV1LayoutFromSizeFieldOBUStream(data []byte) (*av1EncryptionLayout, error) {
	protectedRanges := make([]av1ByteRange, 0, 8)
	protectedLength := 0

	offset := 0
	for offset < len(data) {
		obuType, ext, hasSizeField, ok := parseAV1OBUHeader(data[offset])
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

		payloadLen := 0
		sizeFieldLen := 0
		if hasSizeField {
			val, n, ok := readLeb128(data, offset+headerLen)
			if !ok {
				return nil, fmt.Errorf("invalid leb128 at offset %d", offset+headerLen)
			}
			payloadLen = int(val)
			sizeFieldLen = n
			if offset+headerLen+sizeFieldLen+payloadLen > len(data) {
				return nil, fmt.Errorf("OBU payload extends past end at offset %d", offset)
			}
		} else {
			payloadLen = len(data) - (offset + headerLen)
		}

		payloadStart := offset + headerLen + sizeFieldLen
		payloadEnd := payloadStart + payloadLen

		clearPayloadPrefixLen := 0
		if shouldKeepFirstPayloadByteClear(obuType) {
			if payloadLen > 0 {
				clearPayloadPrefixLen = 1
			}
		}

		protectedStart := payloadStart + clearPayloadPrefixLen
		if protectedStart < payloadEnd {
			protectedRanges = append(protectedRanges, av1ByteRange{start: protectedStart, end: payloadEnd})
			protectedLength += payloadEnd - protectedStart
		}

		offset = payloadEnd
		if !hasSizeField {
			break
		}
	}

	return &av1EncryptionLayout{
		protectedRanges: protectedRanges,
		protectedLength: protectedLength,
	}, nil
}

func computeAV1LayoutFromAnnexB(data []byte) (*av1EncryptionLayout, error) {
	protectedRanges := make([]av1ByteRange, 0, 8)
	protectedLength := 0

	offset := 0
	for offset < len(data) {
		val, n, ok := readLeb128(data, offset)
		if !ok {
			return nil, fmt.Errorf("invalid annex-b leb128 at offset %d", offset)
		}

		obuLen := int(val)
		obuStart := offset + n
		obuEnd := obuStart + obuLen
		if obuEnd > len(data) {
			return nil, fmt.Errorf("annex-b obu extends past end at offset %d", offset)
		}
		if obuLen == 0 {
			offset = obuEnd
			continue
		}

		obuType, ext, hasSizeField, ok := parseAV1OBUHeader(data[obuStart])
		if !ok {
			return nil, fmt.Errorf("invalid annex-b OBU header at offset %d", obuStart)
		}
		headerLen := 1
		if ext {
			headerLen++
		}
		if obuStart+headerLen > obuEnd {
			return nil, fmt.Errorf("truncated annex-b OBU header at offset %d", obuStart)
		}

		sizeFieldLen := 0
		payloadStart := obuStart + headerLen
		if hasSizeField {
			_, n, ok := readLeb128(data, payloadStart)
			if !ok {
				return nil, fmt.Errorf("invalid annex-b inner leb128 at offset %d", payloadStart)
			}
			sizeFieldLen = n
			payloadStart += sizeFieldLen
			if payloadStart > obuEnd {
				return nil, fmt.Errorf("annex-b inner size field exceeds OBU at offset %d", obuStart)
			}
		}

		payloadEnd := obuEnd
		payloadLen := payloadEnd - payloadStart
		clearPayloadPrefixLen := 0
		if shouldKeepFirstPayloadByteClear(obuType) {
			if payloadLen > 0 {
				clearPayloadPrefixLen = 1
			}
		}

		protectedStart := payloadStart + clearPayloadPrefixLen
		if protectedStart < payloadEnd {
			protectedRanges = append(protectedRanges, av1ByteRange{start: protectedStart, end: payloadEnd})
			protectedLength += payloadEnd - protectedStart
		}

		offset = obuEnd
	}

	return &av1EncryptionLayout{
		protectedRanges: protectedRanges,
		protectedLength: protectedLength,
	}, nil
}

func computeAV1LayoutFromRTPPayload(data []byte) (*av1EncryptionLayout, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("rtp payload too short")
	}

	aggregationHeader := data[0]
	if (aggregationHeader & 0x07) != 0 {
		return nil, fmt.Errorf("invalid AV1 RTP aggregation header")
	}

	z := (aggregationHeader & 0x80) != 0
	w := int((aggregationHeader & 0x30) >> 4)

	protectedRanges := make([]av1ByteRange, 0, 8)
	protectedLength := 0

	offset := 1
	obuIndex := 0
	for offset < len(data) {
		isLastOBUElement := w > 0 && obuIndex+1 == w

		obuStart := offset
		obuEnd := len(data)
		if !isLastOBUElement {
			val, n, ok := readLeb128(data, offset)
			if !ok {
				return nil, fmt.Errorf("invalid AV1 RTP OBU length at offset %d", offset)
			}
			obuLen := int(val)
			lenFieldEnd := offset + n
			obuStart = lenFieldEnd
			obuEnd = obuStart + obuLen
			if obuEnd > len(data) {
				return nil, fmt.Errorf("AV1 RTP OBU extends past end at offset %d", offset)
			}
		}

		if obuStart >= obuEnd {
			offset = obuEnd
			obuIndex++
			if w > 0 && obuIndex >= w {
				break
			}
			continue
		}

		if z && obuIndex == 0 {
			protectedRanges = append(protectedRanges, av1ByteRange{start: obuStart, end: obuEnd})
			protectedLength += obuEnd - obuStart
			offset = obuEnd
			obuIndex++
			if w > 0 && obuIndex >= w {
				break
			}
			continue
		}

		obuType, ext, hasSizeField, ok := parseAV1OBUHeader(data[obuStart])
		if !ok {
			return nil, fmt.Errorf("invalid AV1 RTP OBU header at offset %d", obuStart)
		}

		headerLen := 1
		if ext {
			headerLen++
		}
		if obuStart+headerLen > obuEnd {
			return nil, fmt.Errorf("truncated AV1 RTP OBU header at offset %d", obuStart)
		}

		sizeFieldLen := 0
		if hasSizeField {
			_, n, ok := readLeb128(data, obuStart+headerLen)
			if !ok {
				return nil, fmt.Errorf("invalid AV1 RTP OBU inner size at offset %d", obuStart+headerLen)
			}
			sizeFieldLen = n
			if obuStart+headerLen+sizeFieldLen > obuEnd {
				return nil, fmt.Errorf("AV1 RTP OBU inner size exceeds OBU at offset %d", obuStart)
			}
		}

		payloadStart := obuStart + headerLen + sizeFieldLen
		payloadEnd := obuEnd
		payloadLen := payloadEnd - payloadStart

		clearPayloadPrefixLen := 0
		if shouldKeepFirstPayloadByteClear(obuType) {
			if payloadLen > 0 {
				clearPayloadPrefixLen = 1
			}
		}

		protectedStart := payloadStart + clearPayloadPrefixLen
		if protectedStart < payloadEnd {
			protectedRanges = append(protectedRanges, av1ByteRange{start: protectedStart, end: payloadEnd})
			protectedLength += payloadEnd - protectedStart
		}

		offset = obuEnd
		obuIndex++
		if w > 0 && obuIndex >= w {
			break
		}
	}

	return &av1EncryptionLayout{
		protectedRanges: protectedRanges,
		protectedLength: protectedLength,
	}, nil
}

func computeAV1LayoutFromRTXPayload(data []byte) (*av1EncryptionLayout, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("rtx payload too short")
	}

	innerLayout, err := computeAV1LayoutFromRTPPayload(data[2:])
	if err != nil {
		return nil, err
	}

	protectedRanges := make([]av1ByteRange, 0, len(innerLayout.protectedRanges))
	for _, r := range innerLayout.protectedRanges {
		protectedRanges = append(protectedRanges, av1ByteRange{
			start: r.start + 2,
			end:   r.end + 2,
		})
	}

	return &av1EncryptionLayout{
		protectedRanges: protectedRanges,
		protectedLength: sumAV1Ranges(protectedRanges),
	}, nil
}

func shouldKeepFirstPayloadByteClear(obuType byte) bool {
	return obuType == av1OBUTypeFrameHeader || obuType == av1OBUTypeFrame
}

func sumAV1Ranges(ranges []av1ByteRange) int {
	total := 0
	for _, r := range ranges {
		total += r.end - r.start
	}
	return total
}

func isValidAV1Range(r av1ByteRange, dataLen int) bool {
	return r.start >= 0 && r.end >= r.start && r.end <= dataLen
}

func isValidAV1Layout(layout *av1EncryptionLayout, dataLen int) bool {
	if layout == nil {
		return false
	}
	for _, r := range layout.protectedRanges {
		if !isValidAV1Range(r, dataLen) {
			return false
		}
	}
	return sumAV1Ranges(layout.protectedRanges) == layout.protectedLength
}

func av1LayoutSignature(layout *av1EncryptionLayout) string {
	if layout == nil {
		return ""
	}
	s := fmt.Sprintf("len=%d", layout.protectedLength)
	for _, r := range layout.protectedRanges {
		s += fmt.Sprintf(";%d-%d", r.start, r.end)
	}
	return s
}

func parseAV1OBUHeader(b byte) (obuType byte, extensionFlag bool, hasSizeField bool, ok bool) {
	if (b & 0x80) != 0 {
		return 0, false, false, false
	}
	if (b & 0x01) != 0 {
		return 0, false, false, false
	}
	return (b & 0x78) >> 3, (b & 0x04) != 0, (b & 0x02) != 0, true
}

func readLeb128(data []byte, offset int) (uint32, int, bool) {
	var value uint32
	shift := 0
	length := 0

	for offset+length < len(data) {
		b := data[offset+length]
		value += uint32(b&0x7f) << shift
		length++
		if (b & 0x80) == 0 {
			return value, length, true
		}
		shift += 7
		if length >= 5 {
			return 0, 0, false
		}
	}

	return 0, 0, false
}

func writeLeb128(value uint32) []byte {
	var out []byte
	v := value
	for v >= 0x80 {
		out = append(out, byte(v&0x7f)|0x80)
		v >>= 7
	}
	out = append(out, byte(v&0x7f))
	return out
}

func extractRanges(data []byte, ranges []av1ByteRange, totalLength int) []byte {
	out := make([]byte, 0, totalLength)
	for _, r := range ranges {
		if r.start < 0 || r.end < r.start || r.end > len(data) {
			continue
		}
		out = append(out, data[r.start:r.end]...)
	}
	return out
}

func writeRanges(target []byte, ranges []av1ByteRange, source []byte, totalLength int) error {
	if len(source) != totalLength {
		return fmt.Errorf("unexpected protected bytes length: %d, expected %d", len(source), totalLength)
	}
	readOffset := 0
	for _, r := range ranges {
		if r.start < 0 || r.end < r.start || r.end > len(target) {
			return fmt.Errorf("invalid protected range: %d..%d (len=%d)", r.start, r.end, len(target))
		}
		n := r.end - r.start
		copy(target[r.start:r.end], source[readOffset:readOffset+n])
		readOffset += n
	}
	return nil
}
