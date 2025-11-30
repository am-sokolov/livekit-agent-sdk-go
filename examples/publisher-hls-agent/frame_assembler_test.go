package main

import (
	"testing"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
)

func TestFrameAssembler_SingleNALPacket(t *testing.T) {
	fa := NewFrameAssembler()

	// Create a single NAL unit packet (IDR keyframe type 5)
	// Format: [NAL header (1 byte)][NAL payload]
	pkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         true, // End of frame
		},
		Payload: []byte{
			0x65,                   // NAL header: type 5 (IDR)
			0x01, 0x02, 0x03, 0x04, // NAL payload
		},
	}

	frame, ts, complete := fa.AddPacket(pkt)
	assert.True(t, complete, "Single NAL with marker should complete frame")
	assert.Equal(t, uint32(1000), ts, "Timestamp should match")

	// Should have Annex B format: start code + NAL unit
	expected := []byte{
		0x00, 0x00, 0x00, 0x01, // 4-byte start code
		0x65, 0x01, 0x02, 0x03, 0x04, // NAL unit
	}
	assert.Equal(t, expected, frame)
}

func TestFrameAssembler_FUAPackets(t *testing.T) {
	fa := NewFrameAssembler()

	// FU-A start packet
	startPkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{
			0x7C,       // FU indicator: F=0, NRI=3, Type=28 (FU-A)
			0x85,       // FU header: S=1 (start), E=0, Type=5 (IDR)
			0x01, 0x02, // Fragment data
		},
	}

	// FU-A middle packet
	midPkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 2,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{
			0x7C,       // FU indicator
			0x05,       // FU header: S=0, E=0, Type=5
			0x03, 0x04, // Fragment data
		},
	}

	// FU-A end packet
	endPkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 3,
			Timestamp:      1000,
			Marker:         true, // End of frame
		},
		Payload: []byte{
			0x7C,       // FU indicator
			0x45,       // FU header: S=0, E=1 (end), Type=5
			0x05, 0x06, // Fragment data
		},
	}

	// Add packets
	_, _, complete := fa.AddPacket(startPkt)
	assert.False(t, complete, "Start packet should not complete frame")

	_, _, complete = fa.AddPacket(midPkt)
	assert.False(t, complete, "Middle packet should not complete frame")

	frame, ts, complete := fa.AddPacket(endPkt)
	assert.True(t, complete, "End packet should complete frame")
	assert.Equal(t, uint32(1000), ts)

	// Should have Annex B format: start code + reconstructed NAL unit
	// NAL header = FU indicator NRI (0x60) | FU header Type (0x05) = 0x65
	expected := []byte{
		0x00, 0x00, 0x00, 0x01, // 4-byte start code
		0x65,                               // Reconstructed NAL header (IDR)
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // Fragment data
	}
	assert.Equal(t, expected, frame)
}

func TestFrameAssembler_STAPAPacket(t *testing.T) {
	fa := NewFrameAssembler()

	// STAP-A packet with SPS and PPS
	pkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         true,
		},
		Payload: []byte{
			0x78, // STAP-A header: F=0, NRI=3, Type=24
			// First NAL (SPS)
			0x00, 0x03, // Size = 3
			0x67, 0x01, 0x02, // SPS data
			// Second NAL (PPS)
			0x00, 0x02, // Size = 2
			0x68, 0x03, // PPS data
		},
	}

	frame, ts, complete := fa.AddPacket(pkt)
	assert.True(t, complete)
	assert.Equal(t, uint32(1000), ts)

	// Should have Annex B format with start codes for each NAL
	expected := []byte{
		// First NAL (SPS)
		0x00, 0x00, 0x00, 0x01,
		0x67, 0x01, 0x02,
		// Second NAL (PPS)
		0x00, 0x00, 0x00, 0x01,
		0x68, 0x03,
	}
	assert.Equal(t, expected, frame)
}

func TestFrameAssembler_MultiplePacketsSameTimestamp(t *testing.T) {
	fa := NewFrameAssembler()

	// Multiple single NAL packets with same timestamp
	pkt1 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{0x67, 0x01, 0x02}, // SPS
	}

	pkt2 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 2,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{0x68, 0x03}, // PPS
	}

	pkt3 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 3,
			Timestamp:      1000,
			Marker:         true, // End of frame
		},
		Payload: []byte{0x65, 0x04, 0x05}, // IDR
	}

	_, _, complete := fa.AddPacket(pkt1)
	assert.False(t, complete)

	_, _, complete = fa.AddPacket(pkt2)
	assert.False(t, complete)

	frame, _, complete := fa.AddPacket(pkt3)
	assert.True(t, complete)

	// Should have all NALUs with start codes
	expected := []byte{
		0x00, 0x00, 0x00, 0x01, 0x67, 0x01, 0x02, // SPS
		0x00, 0x00, 0x00, 0x01, 0x68, 0x03, // PPS
		0x00, 0x00, 0x00, 0x01, 0x65, 0x04, 0x05, // IDR
	}
	assert.Equal(t, expected, frame)
}

func TestFrameAssembler_OutOfOrderPackets(t *testing.T) {
	fa := NewFrameAssembler()

	// Packets arrive out of order
	pkt2 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 2,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{0x68, 0x02}, // PPS
	}

	pkt1 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{0x67, 0x01}, // SPS
	}

	pkt3 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 3,
			Timestamp:      1000,
			Marker:         true,
		},
		Payload: []byte{0x65, 0x03}, // IDR
	}

	fa.AddPacket(pkt2)
	fa.AddPacket(pkt1)
	frame, _, complete := fa.AddPacket(pkt3)
	assert.True(t, complete)

	// Should be sorted by sequence number
	expected := []byte{
		0x00, 0x00, 0x00, 0x01, 0x67, 0x01, // SPS (seq 1)
		0x00, 0x00, 0x00, 0x01, 0x68, 0x02, // PPS (seq 2)
		0x00, 0x00, 0x00, 0x01, 0x65, 0x03, // IDR (seq 3)
	}
	assert.Equal(t, expected, frame)
}

func TestFrameAssembler_EmptyPayload(t *testing.T) {
	fa := NewFrameAssembler()

	pkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         true,
		},
		Payload: []byte{},
	}

	frame, _, complete := fa.AddPacket(pkt)
	assert.False(t, complete, "Empty payload should not complete frame")
	assert.Nil(t, frame)
}

func TestFrameAssembler_NilPacket(t *testing.T) {
	fa := NewFrameAssembler()

	frame, _, complete := fa.AddPacket(nil)
	assert.False(t, complete)
	assert.Nil(t, frame)
}

func TestFrameAssembler_Clear(t *testing.T) {
	fa := NewFrameAssembler()

	// Add some packets
	pkt := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      1000,
			Marker:         false,
		},
		Payload: []byte{0x67, 0x01},
	}
	fa.AddPacket(pkt)

	// Clear should reset state
	fa.Clear()

	// Should start fresh
	pkt2 := &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 1,
			Timestamp:      2000,
			Marker:         true,
		},
		Payload: []byte{0x65, 0x02},
	}
	frame, ts, complete := fa.AddPacket(pkt2)
	assert.True(t, complete)
	assert.Equal(t, uint32(2000), ts)
	assert.NotNil(t, frame)
}

func TestSeqLess(t *testing.T) {
	tests := []struct {
		name     string
		a, b     uint16
		expected bool
	}{
		{"simple less", 1, 2, true},
		{"simple greater", 2, 1, false},
		{"equal", 5, 5, false},
		{"wraparound a less", 65535, 1, true},     // 65535 < 1 with wraparound
		{"wraparound a greater", 1, 65535, false}, // 1 > 65535 with wraparound
		{"near boundary less", 65530, 5, true},
		{"near boundary greater", 5, 65530, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := seqLess(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}
