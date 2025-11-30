package main

import (
	"bytes"
	"sort"
	"sync"

	"github.com/pion/rtp"
)

// H264 NAL unit types for RTP packet classification (RFC 6184)
const (
	// Single NAL unit types (1-23)
	h264NALTypeSingleMin = 1
	h264NALTypeSingleMax = 23
	// STAP-A (24) - Single-Time Aggregation Packet type A
	h264NALTypeSTAPA = 24
	// FU-A (28) - Fragmentation Unit type A
	h264NALTypeFUA = 28
)

// rtpFragment holds a single RTP packet fragment for frame assembly
type rtpFragment struct {
	seqNum  uint16
	payload []byte
	marker  bool // RTP marker bit indicates end of frame
}

// FrameAssembler collects RTP packets and assembles them into complete H264 frames
// in Annex B format for E2EE decryption.
//
// The JS SDK encrypts complete video frames in Annex B format (with start codes).
// RTP packets use RFC 6184 format (without start codes). This assembler:
// 1. Collects RTP packets by timestamp
// 2. Sorts them by sequence number
// 3. Converts to Annex B format (adds start codes, handles FU-A reassembly)
// 4. Returns complete frames for decryption
type FrameAssembler struct {
	mu        sync.Mutex
	fragments map[uint32][]rtpFragment // keyed by RTP timestamp

	// FU-A state for current frame being assembled
	fuaBuffer   []byte // accumulated FU-A payload
	fuaNRI      byte   // F and NRI bits from FU indicator
	fuaNALType  byte   // NAL type from FU header
	fuaStarted  bool   // have we seen start fragment?
	fuaComplete bool   // have we seen end fragment?
}

// NewFrameAssembler creates a new frame assembler
func NewFrameAssembler() *FrameAssembler {
	return &FrameAssembler{
		fragments: make(map[uint32][]rtpFragment),
	}
}

// AddPacket adds an RTP packet to the assembler.
// Returns (completeFrame, timestamp, complete) where:
//   - completeFrame: the assembled Annex B frame if complete
//   - timestamp: the RTP timestamp of the frame
//   - complete: true if a frame was completed
func (fa *FrameAssembler) AddPacket(pkt *rtp.Packet) ([]byte, uint32, bool) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, 0, false
	}

	fa.mu.Lock()
	defer fa.mu.Unlock()

	ts := pkt.Timestamp
	frag := rtpFragment{
		seqNum:  pkt.SequenceNumber,
		payload: make([]byte, len(pkt.Payload)),
		marker:  pkt.Marker,
	}
	copy(frag.payload, pkt.Payload)

	fa.fragments[ts] = append(fa.fragments[ts], frag)

	// Check if frame is complete (marker bit indicates last packet of frame)
	if pkt.Marker {
		frame := fa.assembleFrame(ts)
		delete(fa.fragments, ts)
		return frame, ts, frame != nil
	}

	return nil, 0, false
}

// assembleFrame converts collected RTP packets into Annex B format
func (fa *FrameAssembler) assembleFrame(ts uint32) []byte {
	frags, ok := fa.fragments[ts]
	if !ok || len(frags) == 0 {
		return nil
	}

	// Sort by sequence number (handle wraparound)
	sort.Slice(frags, func(i, j int) bool {
		return seqLess(frags[i].seqNum, frags[j].seqNum)
	})

	var result bytes.Buffer

	// Reset FU-A state
	fa.fuaBuffer = nil
	fa.fuaNRI = 0
	fa.fuaNALType = 0
	fa.fuaStarted = false
	fa.fuaComplete = false

	for _, frag := range frags {
		if len(frag.payload) == 0 {
			continue
		}

		nalType := frag.payload[0] & 0x1F

		switch {
		case nalType >= h264NALTypeSingleMin && nalType <= h264NALTypeSingleMax:
			// Single NAL unit - add 4-byte start code and the NAL unit
			result.Write([]byte{0x00, 0x00, 0x00, 0x01})
			result.Write(frag.payload)

		case nalType == h264NALTypeSTAPA:
			// STAP-A - split aggregated NALUs
			fa.parseSTAPA(frag.payload, &result)

		case nalType == h264NALTypeFUA:
			// FU-A - handle fragmented NAL unit
			fa.parseFUA(frag.payload, &result)
		}
	}

	// If we have incomplete FU-A data, flush it
	if fa.fuaStarted && len(fa.fuaBuffer) > 0 {
		// Reconstruct NAL header: F and NRI from indicator, type from FU header
		nalHeader := fa.fuaNRI | fa.fuaNALType
		result.Write([]byte{0x00, 0x00, 0x00, 0x01})
		result.WriteByte(nalHeader)
		result.Write(fa.fuaBuffer)
	}

	if result.Len() == 0 {
		return nil
	}

	return result.Bytes()
}

// parseSTAPA parses a STAP-A packet and writes NAL units to result
func (fa *FrameAssembler) parseSTAPA(payload []byte, result *bytes.Buffer) {
	if len(payload) < 3 {
		return
	}

	offset := 1 // Skip STAP-A header
	for offset+2 <= len(payload) {
		nalSize := int(payload[offset])<<8 | int(payload[offset+1])
		offset += 2

		if nalSize <= 0 || offset+nalSize > len(payload) {
			break
		}

		// Add 4-byte start code and NAL unit
		result.Write([]byte{0x00, 0x00, 0x00, 0x01})
		result.Write(payload[offset : offset+nalSize])

		offset += nalSize
	}
}

// parseFUA parses a FU-A packet and accumulates the NAL unit data
func (fa *FrameAssembler) parseFUA(payload []byte, result *bytes.Buffer) {
	if len(payload) < 2 {
		return
	}

	fuIndicator := payload[0]
	fuHeader := payload[1]

	startBit := (fuHeader & 0x80) != 0
	endBit := (fuHeader & 0x40) != 0
	nalType := fuHeader & 0x1F

	// Extract F and NRI bits from FU indicator (upper 3 bits)
	fnri := fuIndicator & 0xE0

	if startBit {
		// Start of fragmented NAL - save the header info
		fa.fuaNRI = fnri
		fa.fuaNALType = nalType
		fa.fuaBuffer = make([]byte, 0, len(payload)*10) // Pre-allocate
		fa.fuaStarted = true
		fa.fuaComplete = false
	}

	if !fa.fuaStarted {
		// Received continuation/end without start - discard
		return
	}

	// Append fragment data (skip FU indicator and FU header)
	if len(payload) > 2 {
		fa.fuaBuffer = append(fa.fuaBuffer, payload[2:]...)
	}

	if endBit {
		// End of fragmented NAL - write complete NAL unit
		nalHeader := fa.fuaNRI | fa.fuaNALType
		result.Write([]byte{0x00, 0x00, 0x00, 0x01})
		result.WriteByte(nalHeader)
		result.Write(fa.fuaBuffer)

		// Reset FU-A state
		fa.fuaBuffer = nil
		fa.fuaStarted = false
		fa.fuaComplete = true
	}
}

// seqLess compares two sequence numbers accounting for wraparound
func seqLess(a, b uint16) bool {
	// Handle wraparound: if difference is > 32768, assume wraparound
	diff := int32(a) - int32(b)
	if diff > 32768 {
		diff -= 65536
	} else if diff < -32768 {
		diff += 65536
	}
	return diff < 0
}

// Cleanup removes old incomplete frames (call periodically to prevent memory leaks)
func (fa *FrameAssembler) Cleanup(maxAge int) {
	fa.mu.Lock()
	defer fa.mu.Unlock()

	// Simple cleanup: if we have too many incomplete frames, clear them all
	// A more sophisticated approach would track timestamps and age
	if len(fa.fragments) > maxAge {
		fa.fragments = make(map[uint32][]rtpFragment)
	}
}

// Clear removes all buffered fragments
func (fa *FrameAssembler) Clear() {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	fa.fragments = make(map[uint32][]rtpFragment)
	fa.fuaBuffer = nil
	fa.fuaStarted = false
}
