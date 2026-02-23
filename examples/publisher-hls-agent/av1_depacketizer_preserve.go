package main

import (
	"errors"
	"fmt"

	"github.com/pion/rtp/codecs/av1/obu"
)

const (
	av1ZMask = byte(0b10000000)
	av1YMask = byte(0b01000000)
	av1WMask = byte(0b00110000)
	av1NMask = byte(0b00001000)
)

var errShortAV1Packet = errors.New("short AV1 RTP payload")

// av1DepacketizerPreserve reconstructs AV1 low-overhead OBU stream while preserving
// all OBU types (including temporal delimiter/tile-list). The stock Pion depacketizer
// drops some OBUs, which can break frame-level E2EE auth when those bytes were covered.
type av1DepacketizerPreserve struct {
	buffer []byte

	Z bool
	Y bool
	N bool
}

//nolint:gocognit,cyclop
func (d *av1DepacketizerPreserve) Unmarshal(payload []byte) ([]byte, error) {
	buff := make([]byte, 0)

	if len(payload) <= 1 {
		return nil, errShortAV1Packet
	}

	obuZ := (payload[0] & av1ZMask) != 0
	obuY := (payload[0] & av1YMask) != 0
	obuCount := int((payload[0] & av1WMask) >> 4)
	obuN := (payload[0] & av1NMask) != 0
	d.Z = obuZ
	d.Y = obuY
	d.N = obuN
	if obuN {
		d.buffer = nil
	}

	// If Z=0, first OBU is not continuation from previous packet.
	if !obuZ && len(d.buffer) > 0 {
		d.buffer = nil
	}

	obuOffset := 0
	for offset := 1; offset < len(payload); obuOffset++ {
		isFirst := obuOffset == 0
		isLast := obuCount != 0 && obuOffset == obuCount-1

		lengthField := 0
		if obuCount == 0 || !isLast {
			obuSizeVal, n, err := obu.ReadLeb128(payload[offset:])
			if err != nil {
				return nil, err
			}
			lengthField = int(obuSizeVal)
			offset += int(n)
			if obuCount == 0 && offset+lengthField == len(payload) {
				isLast = true
			}
		} else {
			// Last OBU element in packet with explicit W count has no external length field.
			lengthField = len(payload) - offset
		}

		if offset+lengthField > len(payload) {
			return nil, fmt.Errorf(
				"%w: OBU size %d + %d offset exceeds payload length %d",
				errShortAV1Packet, lengthField, offset, len(payload),
			)
		}

		var obuBuffer []byte
		if isFirst && obuZ {
			// First element is continuation from previous packet.
			if len(d.buffer) == 0 {
				if isLast {
					break
				}
				offset += lengthField
				continue
			}

			obuBuffer = make([]byte, len(d.buffer)+lengthField)
			copy(obuBuffer, d.buffer)
			copy(obuBuffer[len(d.buffer):], payload[offset:offset+lengthField])
			d.buffer = nil
		} else {
			obuBuffer = payload[offset : offset+lengthField]
		}
		offset += lengthField

		if isLast && obuY {
			// Last element is fragmented into next RTP packet.
			d.buffer = obuBuffer
			break
		}

		if len(obuBuffer) == 0 {
			continue
		}

		obuHeader, err := obu.ParseOBUHeader(obuBuffer)
		if err != nil {
			return nil, err
		}

		// Preserve all OBU types to avoid changing bytes covered by AV1 E2EE auth.
		if obuHeader.HasSizeField {
			obuSize, n, err := obu.ReadLeb128(obuBuffer[obuHeader.Size():])
			if err != nil {
				return nil, err
			}

			sizeFromOBUSize := obuHeader.Size() + int(obuSize) + int(n)
			if lengthField != sizeFromOBUSize {
				return nil, fmt.Errorf(
					"%w: OBU size %d does not match calculated size %d",
					errShortAV1Packet, obuSize, sizeFromOBUSize,
				)
			}

			buff = append(buff, obuBuffer...)
		} else {
			obuHeader.HasSizeField = true
			buff = append(buff, obuHeader.Marshal()...)
			size := len(obuBuffer) - obuHeader.Size()
			buff = append(buff, obu.WriteToLeb128(uint(size))...)
			buff = append(buff, obuBuffer[obuHeader.Size():]...)
		}

		if isLast {
			break
		}
	}

	if obuCount != 0 && obuOffset != obuCount-1 {
		return nil, fmt.Errorf(
			"%w: OBU count %d does not match number of OBUs %d",
			errShortAV1Packet, obuCount, obuOffset,
		)
	}

	return buff, nil
}

func (d *av1DepacketizerPreserve) IsPartitionHead(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	return (payload[0] & av1ZMask) == 0
}

func (d *av1DepacketizerPreserve) IsPartitionTail(marker bool, _ []byte) bool {
	return marker
}
