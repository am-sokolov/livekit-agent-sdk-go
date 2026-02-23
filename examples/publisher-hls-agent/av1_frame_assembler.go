package main

import (
	"sync"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

// AV1FrameAssembler reconstructs complete AV1 samples from RTP packets.
//
// We rely on Pion's samplebuilder to tolerate packet reordering/duplication and
// avoid prematurely cutting a frame at marker-bit boundaries (important for AV1
// SVC/multi-layer streams). The resulting sample data is low-overhead AV1 OBU
// stream with size fields, suitable for AV1 E2EE metadata parsing/decryption.
type AV1FrameAssembler struct {
	mu sync.Mutex

	builder *samplebuilder.SampleBuilder
}

// AV1AssembledFrame represents a single assembled AV1 frame as an OBU stream.
type AV1AssembledFrame struct {
	Data      []byte
	Timestamp uint32
}

// NewAV1FrameAssembler constructs a new AV1FrameAssembler instance.
func NewAV1FrameAssembler(_ string) *AV1FrameAssembler {
	return &AV1FrameAssembler{
		builder: samplebuilder.New(
			512, // tolerate bursty reordering/loss on AV1 SVC streams
			&av1DepacketizerPreserve{},
			90000, // AV1 RTP clock rate
		),
	}
}

// AddPacket adds an RTP packet to the assembler and returns a complete frame when available.
func (a *AV1FrameAssembler) AddPacket(pkt *rtp.Packet) (*AV1AssembledFrame, error) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.builder.Push(pkt.Clone())
	sample := a.builder.Pop()
	if sample == nil || len(sample.Data) == 0 {
		return nil, nil
	}

	return &AV1AssembledFrame{
		Data:      append([]byte(nil), sample.Data...),
		Timestamp: sample.PacketTimestamp,
	}, nil
}

// Flush clears buffered packets and assembled samples.
func (a *AV1FrameAssembler) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.builder.Flush()
	for {
		if a.builder.Pop() == nil {
			break
		}
	}
}
