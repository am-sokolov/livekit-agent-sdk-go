package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/go-gst/go-gst/gst/app"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// ParticipantRecorder records a participant's audio and video streams to HLS format.
//
// It creates a GStreamer pipeline that:
//   - Receives RTP packets (H.264 video, Opus audio)
//   - Optionally transcodes audio to AAC (or keeps Opus)
//   - Muxes to MPEG-TS format
//   - Generates HLS playlists and segments via hlssink
//
// The recorder implements delayed pipeline start to ensure all HLS segments
// begin with valid H.264 keyframes containing SPS/PPS headers.
type ParticipantRecorder struct {
	participant string
	room        string
	outputDir   string
	keepOpus    bool
	logger      *slog.Logger

	pipeline    *gst.Pipeline
	videoAppSrc *app.Source
	audioAppSrc *app.Source
	videoDepay  *gst.Element
	audioDepay  *gst.Element

	videoInitialized bool
	audioInitialized bool
	videoEnded       bool
	audioEnded       bool

	videoReadyOnce           sync.Once
	videoReadyCh             chan struct{}
	videoReady               atomic.Bool
	handshakeReady           atomic.Bool
	recordingActive          atomic.Bool
	recordingKeyframePending atomic.Bool
	pipelineStarted          atomic.Bool
	onVideoReady             func()

	videoPacketCount      int
	videoEmptyPacketCount int
	videoKeyframeCount    int
	videoSPSCount         int
	videoPPSCount         int
	videoPLIRequests      int
	videoTimestampBase    uint32
	videoTimestampInit    bool
	videoLastPTS          gst.ClockTime
	videoLastTimestamp    uint32
	audioPacketCount      int
	audioEmptyPacketCount int
	videoBytesReceived    int64
	audioBytesReceived    int64
	audioTimestampBase    uint32
	audioTimestampInit    bool
	audioLastPTS          gst.ClockTime
	audioLastTimestamp    uint32

	mu        sync.Mutex
	wg        sync.WaitGroup
	stopOnce  sync.Once
	startTime time.Time

	preVideoMu      sync.Mutex
	preVideoPackets []*rtp.Packet
	preAudioMu      sync.Mutex
	preAudioPackets []*rtp.Packet

	s3Uploader *RealtimeS3Uploader // Real-time S3 uploader (nil if disabled)

	e2eeCtx        *E2EEContext    // E2EE decryption context (nil if disabled)
	frameAssembler *FrameAssembler // Frame assembler for E2EE video decryption

	// E2EE raw H264 video path (bypasses RTP depayload)
	e2eeVideoAppSrc *app.Source
	e2eeVideoQueue  *gst.Element
}

// Pre-buffer configuration for video and audio packets.
//
// These buffers store incoming RTP packets before recording activation,
// ensuring the first HLS segment contains both audio and video streams
// when the pipeline starts.
//
// Buffer sizes are chosen to accommodate typical streaming scenarios:
//   - preVideoBufferMax: 300 packets (approximately 10 seconds at 30fps)
//   - preAudioBufferMax: 500 packets (approximately 10 seconds at 50 packets/sec for Opus)
//
// When a buffer fills up, the oldest packet is discarded (FIFO behavior).
// This prevents unbounded memory growth while waiting for recording activation.
//
// The buffers are critical for ensuring HLS segment synchronization:
//  1. Video and audio packets arrive before recording starts
//  2. Packets are buffered until ActivateRecording() is called
//  3. On first keyframe, buffered packets prime the GStreamer pipeline
//  4. This ensures segment 0 contains synchronized audio and video
const (
	preVideoBufferMax = 300
	preAudioBufferMax = 500
)

// RecordingSummary contains statistics and metadata about a completed recording.
type RecordingSummary struct {
	Participant  string        // Participant identity
	Room         string        // Room name
	OutputFile   string        // Local path to output.ts file
	SizeBytes    int64         // Size of output.ts in bytes
	Duration     time.Duration // Recording duration
	Err          error         // Error if recording failed
	VideoPackets int           // Number of video RTP packets processed
	AudioPackets int           // Number of audio RTP packets processed
	Remote       string        // S3 URL if uploaded, empty otherwise
}

// NewParticipantRecorder creates a new recorder for a participant.
//
// It initializes the GStreamer pipeline but does not start it.
// The pipeline will start when the first recording keyframe arrives
// (after ActivateRecording is called and a keyframe is received).
//
// Returns an error if the output directory cannot be created or
// if any GStreamer element fails to initialize.
//
// Each recorder creates a unique temporary directory to support concurrent recordings.
// Directory format: OUTPUT_DIR/room_participant_timestamp
func NewParticipantRecorder(cfg *Config, roomName, participant string, e2eeEnabled bool) (*ParticipantRecorder, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// Create single unique directory for this recording session
	// Format: room_participant_timestamp (flat structure for easy cleanup)
	timestamp := time.Now().Format("20060102-150405.000")
	sessionDir := fmt.Sprintf("%s_%s_%s", roomName, participant, timestamp)
	baseDir := filepath.Join(cfg.OutputDir, sessionDir)
	absDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output directory: %w", err)
	}

	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory %s: %w", absDir, err)
	}

	log.Printf("[%s/%s] created session directory: %s", roomName, participant, absDir)

	// Initialize real-time S3 uploader if enabled
	var s3Uploader *RealtimeS3Uploader
	if cfg.S3RealTimeUpload && cfg.S3.Enabled() {
		uploader, err := NewRealtimeS3Uploader(cfg.S3, roomName, participant, absDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create real-time S3 uploader: %w", err)
		}
		s3Uploader = uploader
	}

	gst.Init(nil)

	pipeline, err := gst.NewPipeline("participant-recorder")
	if err != nil {
		return nil, fmt.Errorf("failed to create GStreamer pipeline: %w", err)
	}

	videoSrc, err := gst.NewElement("appsrc")
	if err != nil {
		return nil, fmt.Errorf("failed to create video appsrc: %w", err)
	}
	_ = videoSrc.SetProperty("is-live", true)
	_ = videoSrc.SetProperty("format", gst.FormatTime)
	_ = videoSrc.SetProperty("do-timestamp", false)
	_ = videoSrc.SetProperty("emit-signals", true)
	_ = videoSrc.SetProperty("block", false)
	_ = videoSrc.SetProperty("stream-type", 0)
	_ = videoSrc.SetProperty("max-bytes", uint64(10*1024*1024))

	videoJitter, err := gst.NewElement("rtpjitterbuffer")
	if err != nil {
		return nil, fmt.Errorf("failed to create video jitterbuffer: %w", err)
	}
	_ = videoJitter.SetProperty("latency", uint(200))
	_ = videoJitter.SetProperty("mode", int(1)) // RTP_JITTER_BUFFER_MODE_NONE to keep RTP timestamps unmodified

	videoDepay, err := gst.NewElement("rtph264depay")
	if err != nil {
		return nil, fmt.Errorf("failed to create rtph264depay: %w", err)
	}

	h264parse, err := gst.NewElement("h264parse")
	if err != nil {
		return nil, fmt.Errorf("failed to create h264parse: %w", err)
	}
	// force SPS/PPS before every keyframe so the resulting HLS segments stay decodable
	_ = h264parse.SetProperty("disable-passthrough", true)
	_ = h264parse.SetProperty("config-interval", int32(-1))

	videoCapsFilter, err := gst.NewElement("capsfilter")
	if err != nil {
		return nil, fmt.Errorf("failed to create video capsfilter: %w", err)
	}
	videoCaps := gst.NewCapsFromString("video/x-h264,stream-format=byte-stream,alignment=au")
	_ = videoCapsFilter.SetProperty("caps", videoCaps)

	videoQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue: %w", err)
	}
	_ = videoQueue.SetProperty("max-size-buffers", uint(0))
	_ = videoQueue.SetProperty("max-size-bytes", uint(0))
	_ = videoQueue.SetProperty("max-size-time", uint64(0))

	// Create E2EE video pipeline elements only when E2EE is enabled
	// This path is used when E2EE is enabled: encrypted RTP → frame assembly → decrypt → raw H264
	var e2eeVideoSrc, e2eeH264Parse, e2eeVideoCapsFilter, e2eeVideoQueue *gst.Element
	if e2eeEnabled {
		e2eeVideoSrc, err = gst.NewElement("appsrc")
		if err != nil {
			return nil, fmt.Errorf("create e2ee video appsrc: %w", err)
		}
		_ = e2eeVideoSrc.SetProperty("is-live", true)
		_ = e2eeVideoSrc.SetProperty("format", gst.FormatTime)
		_ = e2eeVideoSrc.SetProperty("do-timestamp", false)
		_ = e2eeVideoSrc.SetProperty("emit-signals", true)
		_ = e2eeVideoSrc.SetProperty("block", false)
		_ = e2eeVideoSrc.SetProperty("stream-type", 0)
		_ = e2eeVideoSrc.SetProperty("max-bytes", uint64(10*1024*1024))
		// Set caps for raw H264 Annex B format
		e2eeCaps := gst.NewCapsFromString("video/x-h264,stream-format=byte-stream,alignment=au")
		_ = e2eeVideoSrc.SetProperty("caps", e2eeCaps)

		e2eeH264Parse, err = gst.NewElement("h264parse")
		if err != nil {
			return nil, fmt.Errorf("create e2ee h264parse: %w", err)
		}
		_ = e2eeH264Parse.SetProperty("disable-passthrough", true)
		_ = e2eeH264Parse.SetProperty("config-interval", int32(-1))

		e2eeVideoCapsFilter, err = gst.NewElement("capsfilter")
		if err != nil {
			return nil, fmt.Errorf("create e2ee video capsfilter: %w", err)
		}
		e2eeVideoCapsValue := gst.NewCapsFromString("video/x-h264,stream-format=byte-stream,alignment=au")
		_ = e2eeVideoCapsFilter.SetProperty("caps", e2eeVideoCapsValue)

		e2eeVideoQueue, err = gst.NewElement("queue")
		if err != nil {
			return nil, fmt.Errorf("create e2ee video queue: %w", err)
		}
		_ = e2eeVideoQueue.SetProperty("max-size-buffers", uint(0))
		_ = e2eeVideoQueue.SetProperty("max-size-bytes", uint(0))
		_ = e2eeVideoQueue.SetProperty("max-size-time", uint64(0))
	}

	audioSrc, err := gst.NewElement("appsrc")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio appsrc: %w", err)
	}
	_ = audioSrc.SetProperty("is-live", true)
	_ = audioSrc.SetProperty("format", gst.FormatTime)
	_ = audioSrc.SetProperty("do-timestamp", false)
	_ = audioSrc.SetProperty("emit-signals", true)
	_ = audioSrc.SetProperty("block", false)
	_ = audioSrc.SetProperty("stream-type", 0)
	_ = audioSrc.SetProperty("max-bytes", uint64(2*1024*1024))

	audioJitter, err := gst.NewElement("rtpjitterbuffer")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio jitterbuffer: %w", err)
	}
	_ = audioJitter.SetProperty("latency", uint(200))
	_ = audioJitter.SetProperty("mode", int(1)) // Disable jitterbuffer resync to preserve relative timestamps

	audioDepay, err := gst.NewElement("rtpopusdepay")
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpopusdepay: %w", err)
	}

	// Audio processing pipeline depends on KeepOpus configuration
	var opusDec, audioConvert, aacEnc, aacParse, opusParse *gst.Element
	if cfg.KeepOpus {
		// Keep Opus without transcoding
		opusParse, err = gst.NewElement("opusparse")
		if err != nil {
			return nil, fmt.Errorf("failed to create opusparse: %w", err)
		}
	} else {
		// Transcode Opus to AAC
		opusDec, err = gst.NewElement("opusdec")
		if err != nil {
			return nil, fmt.Errorf("failed to create opusdec: %w", err)
		}

		audioConvert, err = gst.NewElement("audioconvert")
		if err != nil {
			return nil, fmt.Errorf("failed to create audioconvert: %w", err)
		}

		aacEnc, err = gst.NewElement("avenc_aac")
		if err != nil {
			return nil, fmt.Errorf("failed to create avenc_aac: %w", err)
		}
		_ = aacEnc.SetProperty("bitrate", uint(128000))

		aacParse, err = gst.NewElement("aacparse")
		if err != nil {
			return nil, fmt.Errorf("failed to create aacparse: %w", err)
		}
	}

	audioQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue: %w", err)
	}
	_ = audioQueue.SetProperty("max-size-buffers", uint(0))
	_ = audioQueue.SetProperty("max-size-bytes", uint(0))
	_ = audioQueue.SetProperty("max-size-time", uint64(0))

	mpegtsmux, err := gst.NewElement("mpegtsmux")
	if err != nil {
		return nil, fmt.Errorf("failed to create mpegtsmux: %w", err)
	}
	_ = mpegtsmux.SetProperty("alignment", int64(7))
	_ = mpegtsmux.SetProperty("start-time-selection", int64(0)) // Force timestamps to start at zero
	_ = mpegtsmux.SetProperty("start-time", uint64(0))

	muxQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create mux queue: %w", err)
	}

	outputTee, err := gst.NewElement("tee")
	if err != nil {
		return nil, fmt.Errorf("failed to create tee: %w", err)
	}

	tsQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create ts queue: %w", err)
	}

	tsSink, err := gst.NewElement("filesink")
	if err != nil {
		return nil, fmt.Errorf("failed to create filesink: %w", err)
	}
	_ = tsSink.SetProperty("location", filepath.Join(absDir, "output.ts"))
	_ = tsSink.SetProperty("sync", false)
	_ = tsSink.SetProperty("async", false)

	hlsQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create hls queue: %w", err)
	}

	hlsSink, err := gst.NewElement("hlssink")
	if err != nil {
		return nil, fmt.Errorf("failed to create hlssink: %w", err)
	}
	_ = hlsSink.SetProperty("location", filepath.Join(absDir, "segment%05d.ts"))
	_ = hlsSink.SetProperty("playlist-location", filepath.Join(absDir, "playlist.m3u8"))
	segmentDuration := cfg.SegmentDurationSecs
	if segmentDuration <= 0 {
		segmentDuration = 2
	}
	_ = hlsSink.SetProperty("target-duration", uint(segmentDuration))
	_ = hlsSink.SetProperty("max-files", uint(0))
	_ = hlsSink.SetProperty("playlist-length", uint(0))

	// Build elements list based on audio codec configuration
	elements := []*gst.Element{
		videoSrc, videoJitter, videoDepay, h264parse, videoCapsFilter, videoQueue,
		audioSrc, audioJitter, audioDepay,
	}

	// Add E2EE video path elements only when E2EE is enabled
	if e2eeEnabled {
		elements = append(elements, e2eeVideoSrc, e2eeH264Parse, e2eeVideoCapsFilter, e2eeVideoQueue)
	}

	if cfg.KeepOpus {
		elements = append(elements, opusParse)
	} else {
		elements = append(elements, opusDec, audioConvert, aacEnc, aacParse)
	}
	elements = append(elements,
		audioQueue,
		mpegtsmux, muxQueue, outputTee,
		tsQueue, tsSink,
		hlsQueue, hlsSink,
	)

	for _, elem := range elements {
		if err := pipeline.Add(elem); err != nil {
			return nil, fmt.Errorf("failed to add %s to pipeline: %w", elem.GetName(), err)
		}
	}

	if err := gst.ElementLinkMany(videoSrc, videoJitter, videoDepay, h264parse, videoCapsFilter, videoQueue); err != nil {
		return nil, fmt.Errorf("failed to link video chain: %w", err)
	}

	// Link E2EE video pipeline (raw H264 path) only when E2EE is enabled
	if e2eeEnabled {
		if err := gst.ElementLinkMany(e2eeVideoSrc, e2eeH264Parse, e2eeVideoCapsFilter, e2eeVideoQueue); err != nil {
			return nil, fmt.Errorf("link e2ee video chain: %w", err)
		}
	}

	// Link audio pipeline based on codec configuration
	if cfg.KeepOpus {
		if err := gst.ElementLinkMany(audioSrc, audioJitter, audioDepay, opusParse, audioQueue); err != nil {
			return nil, fmt.Errorf("failed to link audio chain (opus): %w", err)
		}
	} else {
		if err := gst.ElementLinkMany(audioSrc, audioJitter, audioDepay, opusDec, audioConvert, aacEnc, aacParse, audioQueue); err != nil {
			return nil, fmt.Errorf("failed to link audio chain (aac): %w", err)
		}
	}

	// mpegtsmux requires request pads, cannot use ElementLinkMany
	videoMuxPad := mpegtsmux.GetRequestPad("sink_%d")
	if videoMuxPad == nil {
		return nil, fmt.Errorf("failed to get request pad from mpegtsmux for video")
	}
	videoQueueSrc := videoQueue.GetStaticPad("src")
	if videoQueueSrc == nil {
		return nil, fmt.Errorf("failed to get src pad from video queue")
	}
	if linkRet := videoQueueSrc.Link(videoMuxPad); linkRet != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link video queue to mux: %s", linkRet.String())
	}

	// Link E2EE video queue to muxer only when E2EE is enabled
	if e2eeEnabled {
		e2eeVideoMuxPad := mpegtsmux.GetRequestPad("sink_%d")
		if e2eeVideoMuxPad == nil {
			return nil, fmt.Errorf("get request pad from mpegtsmux for e2ee video")
		}
		e2eeVideoQueueSrc := e2eeVideoQueue.GetStaticPad("src")
		if e2eeVideoQueueSrc == nil {
			return nil, fmt.Errorf("get src pad from e2ee video queue")
		}
		if linkRet := e2eeVideoQueueSrc.Link(e2eeVideoMuxPad); linkRet != gst.PadLinkOK {
			return nil, fmt.Errorf("link e2ee video queue to mux: %s", linkRet.String())
		}
	}

	audioMuxPad := mpegtsmux.GetRequestPad("sink_%d")
	if audioMuxPad == nil {
		return nil, fmt.Errorf("failed to get request pad from mpegtsmux for audio")
	}
	audioQueueSrc := audioQueue.GetStaticPad("src")
	if audioQueueSrc == nil {
		return nil, fmt.Errorf("failed to get src pad from audio queue")
	}
	if linkRet := audioQueueSrc.Link(audioMuxPad); linkRet != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link audio queue to mux: %s", linkRet.String())
	}

	if err := gst.ElementLinkMany(mpegtsmux, muxQueue, outputTee); err != nil {
		return nil, fmt.Errorf("failed to link mux branch: %w", err)
	}

	if err := gst.ElementLinkMany(tsQueue, tsSink); err != nil {
		return nil, fmt.Errorf("failed to link ts branch: %w", err)
	}
	if err := gst.ElementLinkMany(hlsQueue, hlsSink); err != nil {
		return nil, fmt.Errorf("failed to link hls branch: %w", err)
	}

	teePad1 := outputTee.GetRequestPad("src_%u")
	tsQueueSink := tsQueue.GetStaticPad("sink")
	if linkRet := teePad1.Link(tsQueueSink); linkRet != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link tee to ts queue: %s", linkRet.String())
	}

	teePad2 := outputTee.GetRequestPad("src_%u")
	hlsQueueSink := hlsQueue.GetStaticPad("sink")
	if linkRet := teePad2.Link(hlsQueueSink); linkRet != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link tee to hls queue: %s", linkRet.String())
	}

	_ = os.Setenv("GST_DEBUG_DUMP_DOT_DIR", absDir)

	recorder := &ParticipantRecorder{
		logger:         logger,
		participant:    participant,
		room:           roomName,
		outputDir:      absDir,
		keepOpus:       cfg.KeepOpus,
		pipeline:       pipeline,
		videoAppSrc:    app.SrcFromElement(videoSrc),
		audioAppSrc:    app.SrcFromElement(audioSrc),
		videoDepay:     videoDepay,
		audioDepay:     audioDepay,
		startTime:      time.Now(),
		videoReadyCh:   make(chan struct{}),
		s3Uploader:     s3Uploader,
		frameAssembler: NewFrameAssembler(),
		e2eeVideoQueue: e2eeVideoQueue, // nil if E2EE disabled
	}

	// Set E2EE video appsrc only when E2EE is enabled
	if e2eeEnabled && e2eeVideoSrc != nil {
		recorder.e2eeVideoAppSrc = app.SrcFromElement(e2eeVideoSrc)
	}

	audioCodec := "AAC"
	if cfg.KeepOpus {
		audioCodec = "Opus"
	}
	log.Printf("[%s/%s] recorder initialized with H.264 + %s", roomName, participant, audioCodec)

	bus := pipeline.GetPipelineBus()
	bus.AddWatch(func(msg *gst.Message) bool {
		switch msg.Type() {
		case gst.MessageError:
			gerr := msg.ParseError()
			log.Printf("[%s] GStreamer error: %v (debug: %s)", recorder.logPrefix(), gerr.Error(), gerr.DebugString())
			return false
		case gst.MessageWarning:
			gw := msg.ParseWarning()
			log.Printf("[%s] GStreamer warning: %v", recorder.logPrefix(), gw.Error())
		case gst.MessageEOS:
			log.Printf("[%s] GStreamer EOS received", recorder.logPrefix())
			return false
		}
		return true
	})

	return recorder, nil
}

// logPrefix returns a standardized log prefix for this recorder.
//
// Format: "room/participant"
//
// This prefix is prepended to all log messages from this recorder instance,
// making it easy to correlate log entries with specific recording sessions
// when multiple participants are being recorded simultaneously.
//
// Example output: "test-room/participant-123"
func (r *ParticipantRecorder) logPrefix() string {
	return fmt.Sprintf("%s/%s", r.room, r.participant)
}

// SetE2EEContext sets the E2EE decryption context for this recorder.
// When set, incoming audio and video RTP payloads will be decrypted
// before being passed to the GStreamer pipeline.
//
// This must be called before tracks are attached. The context should
// be created using NewE2EEContext with the room's SIF trailer.
func (r *ParticipantRecorder) SetE2EEContext(ctx *E2EEContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.e2eeCtx = ctx
}

// E2EEEnabled returns true if E2EE decryption is enabled for this recorder.
func (r *ParticipantRecorder) E2EEEnabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.e2eeCtx != nil && r.e2eeCtx.Enabled()
}

// SetOnVideoReady sets a callback to be invoked when the first video keyframe
// is received (handshake ready). If handshake is already complete, the callback
// is invoked immediately.
func (r *ParticipantRecorder) SetOnVideoReady(cb func()) {
	r.mu.Lock()
	r.onVideoReady = cb
	r.mu.Unlock()
	if cb != nil && r.handshakeReady.Load() {
		cb()
	}
}

// HandshakeReady returns true if the recorder has received the first video keyframe.
// This indicates that H.264 parameters (SPS/PPS) are established and recording can begin.
func (r *ParticipantRecorder) HandshakeReady() bool {
	return r.handshakeReady.Load()
}

// ActivateRecording enables recording. The GStreamer pipeline will start
// at the next keyframe, and all subsequent packets will be recorded to HLS.
//
// This resets packet counters and timestamp bases, and clears pre-buffers
// to ensure recording starts from the next keyframe (not a stale buffered one).
func (r *ParticipantRecorder) ActivateRecording() {
	r.recordingActive.Store(true)
	r.recordingKeyframePending.Store(true)
	r.mu.Lock()
	r.videoTimestampInit = false
	r.audioTimestampInit = false
	r.videoPacketCount = 0
	r.videoEmptyPacketCount = 0
	r.videoKeyframeCount = 0
	r.videoSPSCount = 0
	r.videoPPSCount = 0
	r.videoPLIRequests = 0
	r.videoBytesReceived = 0
	r.videoLastPTS = 0
	r.videoLastTimestamp = 0
	r.audioPacketCount = 0
	r.audioEmptyPacketCount = 0
	r.audioBytesReceived = 0
	r.audioLastPTS = 0
	r.audioLastTimestamp = 0
	r.mu.Unlock()

	// Clear pre-buffers to discard warm-up packets
	// Fresh packets will be buffered while waiting for the next keyframe
	// This ensures segment 0 only contains packets from the actual recording
	r.clearPreVideoBuffer()
	r.clearPreAudioBuffer()
}

// Start prepares the recorder for operation.
//
// Note: This does NOT start the GStreamer pipeline. The pipeline will be started
// automatically when the first recording keyframe arrives after ActivateRecording.
// This delayed start ensures all HLS segments begin with valid H.264 keyframes.
func (r *ParticipantRecorder) Start() error {
	log.Printf("[%s] GStreamer pipeline ready (will start on first recording keyframe)", r.logPrefix())
	// Pipeline will be started when first recording keyframe arrives
	// This prevents invalid HLS segments from being created before recording begins
	return nil
}

// H.264 NAL unit types used to detect keyframes and parameter sets.
//
// These constants define NAL unit type values as specified in ITU-T H.264 / ISO/IEC 14496-10.
//
// Parameter sets (must precede video data for decoding):
//   - nalUnitTypeSPS (7): Sequence Parameter Set
//     Contains global codec parameters (resolution, profile, level, etc.)
//   - nalUnitTypePPS (8): Picture Parameter Set
//     Contains picture-specific parameters (entropy coding mode, slice groups, etc.)
//
// Video data:
//   - nalUnitTypeIDR (5): IDR (Instantaneous Decoder Refresh) keyframe
//     Intra-coded frame that can be decoded independently (no dependencies on previous frames)
//
// RTP packetization (RFC 6184):
//   - nalUnitTypeSTAPA (24): Single-Time Aggregation Packet Type A
//     Multiple NAL units in a single RTP packet (used to reduce overhead)
//   - nalUnitTypeFUA (28): Fragmentation Unit Type A
//     Large NAL unit fragmented across multiple RTP packets
//
// Keyframe detection logic:
// A keyframe is identified by the presence of SPS, PPS, or IDR NAL units.
// For HLS recording, we must wait for a keyframe before starting the pipeline
// to ensure all segments begin with decodable video (no P-frame dependencies).
//
// The NAL unit type is encoded in the lower 5 bits of the first payload byte:
//
//	nalType = payload[0] & 0x1F
const (
	nalUnitTypeSPS   = 7  // Sequence Parameter Set
	nalUnitTypePPS   = 8  // Picture Parameter Set
	nalUnitTypeIDR   = 5  // IDR (Instantaneous Decoder Refresh) keyframe
	nalUnitTypeSTAPA = 24 // Single-time Aggregation Packet Type A (multiple NAL units)
	nalUnitTypeFUA   = 28 // Fragmentation Unit Type A (fragmented NAL unit)
)

// isH264Keyframe detects whether an RTP payload contains or references an H.264 keyframe.
//
// This function inspects the NAL unit type indicator byte and handles:
//   - Single NAL units: SPS (7), PPS (8), IDR (5)
//   - STAP-A packets: Searches aggregated NAL units for SPS/PPS/IDR
//   - FU-A packets: Checks if the start fragment contains an IDR NAL unit
//
// Returns true if the payload contains keyframe data (SPS, PPS, or IDR).
func isH264Keyframe(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}

	nalType := payload[0] & 0x1F

	switch nalType {
	case nalUnitTypeSPS, nalUnitTypePPS, nalUnitTypeIDR:
		return true
	case nalUnitTypeSTAPA:
		offset := 1
		for offset < len(payload) {
			if offset+2 > len(payload) {
				break
			}
			nalSize := int(payload[offset])<<8 | int(payload[offset+1])
			offset += 2

			if offset+nalSize > len(payload) {
				break
			}
			if nalSize > 0 {
				innerNalType := payload[offset] & 0x1F
				if innerNalType == nalUnitTypeSPS || innerNalType == nalUnitTypePPS || innerNalType == nalUnitTypeIDR {
					return true
				}
			}
			offset += nalSize
		}
		return false
	case nalUnitTypeFUA:
		if len(payload) < 2 {
			return false
		}
		fuHeader := payload[1]
		startBit := (fuHeader & 0x80) != 0
		nalType := fuHeader & 0x1F
		return startBit && nalType == nalUnitTypeIDR
	default:
		return false
	}
}

// parseSpropParameterSets extracts H.264 parameter sets (SPS/PPS) from the SDP fmtp line.
//
// The fmtp string (e.g., "sprop-parameter-sets=Z0IAH...=,aM4G8g==") contains base64-encoded
// SPS and PPS NAL units. This function decodes them and returns the raw NAL unit bytes.
//
// Returns nil if fmtp is empty or if sprop-parameter-sets is not present.
func parseSpropParameterSets(fmtp string) [][]byte {
	if fmtp == "" {
		return nil
	}
	params := strings.Split(fmtp, ";")
	for _, param := range params {
		param = strings.TrimSpace(param)
		if !strings.HasPrefix(param, "sprop-parameter-sets=") {
			continue
		}
		value := strings.TrimPrefix(param, "sprop-parameter-sets=")
		parts := strings.Split(value, ",")
		var nalUnits [][]byte
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(part)
			if err != nil {
				log.Printf("failed to decode sprop parameter set %q: %v", part, err)
				continue
			}
			nalUnits = append(nalUnits, data)
		}
		return nalUnits
	}
	return nil
}

// classifyNALUnit determines if a NAL unit is an SPS or PPS.
//
// Returns:
//   - (true, false) if the NAL unit is SPS
//   - (false, true) if the NAL unit is PPS
//   - (false, false) otherwise
func classifyNALUnit(nal []byte) (bool, bool) {
	if len(nal) == 0 {
		return false, false
	}
	switch nal[0] & 0x1F {
	case nalUnitTypeSPS:
		return true, false
	case nalUnitTypePPS:
		return false, true
	default:
		return false, false
	}
}

// detectParameterSets scans an RTP payload for H.264 parameter sets (SPS/PPS).
//
// This function handles:
//   - Single NAL units (SPS=7, PPS=8)
//   - STAP-A packets containing aggregated NAL units
//   - FU-A packets containing fragmented NAL units (only start fragments)
//
// Returns:
//   - (sps bool, pps bool) indicating whether SPS and/or PPS were detected
func detectParameterSets(payload []byte) (bool, bool) {
	if len(payload) == 0 {
		return false, false
	}
	switch payload[0] & 0x1F {
	case nalUnitTypeSPS:
		return true, false
	case nalUnitTypePPS:
		return false, true
	case nalUnitTypeSTAPA:
		var sps, pps bool
		offset := 1
		for offset+2 <= len(payload) {
			nalSize := int(payload[offset])<<8 | int(payload[offset+1])
			offset += 2
			if nalSize <= 0 || offset+nalSize > len(payload) {
				break
			}
			nsps, npps := classifyNALUnit(payload[offset : offset+nalSize])
			sps = sps || nsps
			pps = pps || npps
			offset += nalSize
		}
		return sps, pps
	case nalUnitTypeFUA:
		if len(payload) < 2 {
			return false, false
		}
		fuHeader := payload[1]
		startBit := (fuHeader & 0x80) != 0
		if !startBit {
			return false, false
		}
		nalType := fuHeader & 0x1F
		switch nalType {
		case nalUnitTypeSPS:
			return true, false
		case nalUnitTypePPS:
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

// initVideoCaps initializes the GStreamer video appsrc element's capabilities.
//
// This function must be called before pushing video packets to the appsrc.
// It configures the RTP stream parameters that GStreamer needs to properly
// decode the incoming H.264 video stream.
//
// Caps format:
//
//	application/x-rtp,media=video,encoding-name=H264,clock-rate=90000,payload=<PT>
//
// Parameters explained:
//   - media=video: Indicates this is a video stream
//   - encoding-name=H264: Codec is H.264 (AVC)
//   - clock-rate=90000: RTP timestamp clock rate in Hz (H.264 standard)
//   - payload=<PT>: RTP payload type from the track's codec parameters
//
// Initialization is idempotent (only occurs once per recorder instance).
// Subsequent calls are no-ops to prevent re-initializing the pipeline.
//
// Thread-safety: Protected by r.mu mutex.
//
// Parameters:
//   - payloadType: RTP payload type number from the WebRTC track
func (r *ParticipantRecorder) initVideoCaps(payloadType uint8) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.videoInitialized {
		return
	}
	capsStr := fmt.Sprintf("application/x-rtp,media=video,encoding-name=H264,clock-rate=90000,payload=%d", payloadType)
	caps := gst.NewCapsFromString(capsStr)
	_ = r.videoAppSrc.SetProperty("caps", caps)
	r.videoInitialized = true
	log.Printf("[%s] video caps initialized: %s", r.logPrefix(), capsStr)
}

// enqueuePreVideoPacket adds a video RTP packet to the pre-recording buffer.
//
// This buffer stores video packets that arrive before recording is activated,
// ensuring the first HLS segment contains synchronized audio and video when
// the GStreamer pipeline starts.
//
// Buffering strategy:
//  1. Clone the incoming packet (to avoid mutation by the caller)
//  2. If buffer is full (300 packets), discard oldest packet (FIFO)
//  3. Append new packet to buffer
//
// The pre-buffer is used during two critical phases:
//   - Pre-handshake: Buffering while waiting for first keyframe
//   - Pre-recording: Buffering while waiting for ActivateRecording() call
//
// When recording starts, buffered packets are drained via dequeuePreVideoPacket()
// to prime the GStreamer pipeline before live packets are pushed.
//
// Thread-safety: Protected by r.preVideoMu mutex.
//
// Parameters:
//   - pkt: RTP packet to buffer (nil packets are ignored)
func (r *ParticipantRecorder) enqueuePreVideoPacket(pkt *rtp.Packet) {
	if pkt == nil {
		return
	}
	clone := pkt.Clone()
	if clone == nil {
		return
	}
	r.preVideoMu.Lock()
	if len(r.preVideoPackets) >= preVideoBufferMax {
		r.preVideoPackets[0] = nil
		r.preVideoPackets = r.preVideoPackets[1:]
	}
	r.preVideoPackets = append(r.preVideoPackets, clone)
	r.preVideoMu.Unlock()
}

// dequeuePreVideoPacket removes and returns the oldest video packet from the pre-buffer.
//
// This function is called during recording activation to drain the pre-buffer
// and prime the GStreamer pipeline with buffered packets. By pushing buffered
// packets first, we ensure the first HLS segment contains synchronized audio
// and video streams.
//
// Dequeue behavior:
//   - Returns oldest packet (FIFO ordering)
//   - Removes packet from buffer
//   - Sets removed slot to nil (helps GC)
//   - Returns nil if buffer is empty
//
// Typical usage pattern:
//
//	for {
//	    pkt := r.dequeuePreVideoPacket()
//	    if pkt == nil {
//	        break
//	    }
//	    r.pushVideoPacket(pkt)
//	}
//
// Thread-safety: Protected by r.preVideoMu mutex.
//
// Returns:
//   - RTP packet from buffer, or nil if buffer is empty
func (r *ParticipantRecorder) dequeuePreVideoPacket() *rtp.Packet {
	r.preVideoMu.Lock()
	defer r.preVideoMu.Unlock()
	if len(r.preVideoPackets) == 0 {
		return nil
	}
	pkt := r.preVideoPackets[0]
	r.preVideoPackets[0] = nil
	r.preVideoPackets = r.preVideoPackets[1:]
	return pkt
}

// clearPreVideoBuffer discards all buffered video packets.
//
// This function is called during cleanup to free memory and prevent
// stale packets from being used in future recordings.
//
// Memory management:
//   - Sets all packet references to nil (helps GC reclaim memory)
//   - Resets slice to nil
//
// Calling this function is typically done:
//   - During Stop() to release resources
//   - After ActivateRecording() if starting fresh (currently commented out)
//
// Thread-safety: Protected by r.preVideoMu mutex.
func (r *ParticipantRecorder) clearPreVideoBuffer() {
	r.preVideoMu.Lock()
	for i := range r.preVideoPackets {
		r.preVideoPackets[i] = nil
	}
	r.preVideoPackets = nil
	r.preVideoMu.Unlock()
}

// enqueuePreAudioPacket adds an audio RTP packet to the pre-recording buffer.
//
// This buffer stores audio packets that arrive before recording is activated.
// Audio buffering is critical because:
//   - Audio packets arrive continuously during handshake/warmup phase
//   - Recording can only start on a video keyframe
//   - Without buffering, early audio would be lost
//
// Buffering strategy (same as video):
//  1. Clone the incoming packet (to avoid mutation by the caller)
//  2. If buffer is full (500 packets), discard oldest packet (FIFO)
//  3. Append new packet to buffer
//
// The audio pre-buffer complements the video pre-buffer:
//   - Video buffer: Ensures keyframe-aligned start
//   - Audio buffer: Ensures no audio gaps when recording starts
//
// Thread-safety: Protected by r.preAudioMu mutex.
//
// Parameters:
//   - pkt: RTP packet to buffer (nil packets are ignored)
func (r *ParticipantRecorder) enqueuePreAudioPacket(pkt *rtp.Packet) {
	if pkt == nil {
		return
	}
	clone := pkt.Clone()
	if clone == nil {
		return
	}
	r.preAudioMu.Lock()
	if len(r.preAudioPackets) >= preAudioBufferMax {
		r.preAudioPackets[0] = nil
		r.preAudioPackets = r.preAudioPackets[1:]
	}
	r.preAudioPackets = append(r.preAudioPackets, clone)
	r.preAudioMu.Unlock()
}

// dequeuePreAudioPacket removes and returns the oldest audio packet from the pre-buffer.
//
// This function is called during recording activation to drain the audio pre-buffer
// after the video pipeline has been primed with keyframe packets. The audio packets
// are pushed to GStreamer only after video packets to ensure proper stream alignment
// in the muxer.
//
// Sequencing with video:
//  1. First recording keyframe arrives
//  2. Video pipeline is primed with buffered video packets
//  3. THEN audio pipeline starts (this function is called)
//  4. Result: First HLS segment has both audio and video
//
// Thread-safety: Protected by r.preAudioMu mutex.
//
// Returns:
//   - RTP packet from buffer, or nil if buffer is empty
func (r *ParticipantRecorder) dequeuePreAudioPacket() *rtp.Packet {
	r.preAudioMu.Lock()
	defer r.preAudioMu.Unlock()
	if len(r.preAudioPackets) == 0 {
		return nil
	}
	pkt := r.preAudioPackets[0]
	r.preAudioPackets[0] = nil
	r.preAudioPackets = r.preAudioPackets[1:]
	return pkt
}

// clearPreAudioBuffer discards all buffered audio packets.
//
// This function mirrors clearPreVideoBuffer and is called during cleanup
// to free memory and prevent stale audio packets from being used.
//
// Memory management:
//   - Sets all packet references to nil (helps GC reclaim memory)
//   - Resets slice to nil
//
// Thread-safety: Protected by r.preAudioMu mutex.
func (r *ParticipantRecorder) clearPreAudioBuffer() {
	r.preAudioMu.Lock()
	for i := range r.preAudioPackets {
		r.preAudioPackets[i] = nil
	}
	r.preAudioPackets = nil
	r.preAudioMu.Unlock()
}

// pushE2EEVideoFrame pushes a decrypted H264 frame in Annex B format to the E2EE video appsrc.
//
// This function is used for E2EE video when we have complete decrypted frames
// rather than individual RTP packets. The frame data is in Annex B format
// (with start codes) and can be pushed directly to h264parse.
//
// Parameters:
//   - frameData: Decrypted H264 frame in Annex B format
//   - timestamp: RTP timestamp (32-bit, 90kHz clock)
//
// Returns:
//   - nil on success
//   - error if appsrc rejects the buffer
func (r *ParticipantRecorder) pushE2EEVideoFrame(frameData []byte, timestamp uint32) error {
	pts, relative := r.videoClockTime(timestamp)

	buffer := gst.NewBufferFromBytes(frameData)
	buffer.SetPresentationTimestamp(pts)

	r.mu.Lock()
	r.videoLastPTS = pts
	r.videoLastTimestamp = relative
	r.mu.Unlock()

	if flow := r.e2eeVideoAppSrc.PushBuffer(buffer); flow != gst.FlowOK {
		if flow == gst.FlowFlushing {
			return fmt.Errorf("e2ee video appsrc flushing")
		}
		return fmt.Errorf("e2ee video appsrc push failed: %s", flow.String())
	}
	return nil
}

// pushVideoPacket converts an RTP packet to a GStreamer buffer and pushes it to the video appsrc.
//
// This function performs four critical operations:
//  1. Timestamp normalization: Converts absolute RTP timestamp to relative (base-0)
//  2. PTS calculation: Converts RTP timestamp to GStreamer ClockTime (nanoseconds)
//  3. RTP marshaling: Serializes the RTP packet to wire format
//  4. Buffer push: Sends the buffer to GStreamer's video appsrc element
//
// Timestamp handling:
// RTP timestamps are 32-bit values that increment continuously. For HLS recording,
// we need timestamps to start at 0 for the first packet. This function calls
// videoClockTime() to normalize timestamps and convert from 90kHz clock to nanoseconds.
//
// Example:
//
//	First packet:  RTP TS = 3840000000 → Relative TS = 0 → PTS = 0ns
//	Second packet: RTP TS = 3840003000 → Relative TS = 3000 → PTS = 33333333ns (33.3ms at 30fps)
//
// GStreamer buffer properties:
//   - Data: Marshaled RTP packet (header + payload)
//   - PTS: Presentation timestamp in nanoseconds (for muxer synchronization)
//
// Error handling:
//   - Marshal failure: Returns error (malformed RTP packet)
//   - FlowFlushing: Pipeline is shutting down (expected during Stop())
//   - Other flow errors: Unexpected pipeline state (logged and returned)
//
// Thread-safety:
//   - videoClockTime() is protected by r.mu
//   - videoLastPTS/videoLastTimestamp updates are protected by r.mu
//
// Parameters:
//   - pkt: RTP packet to push (modified in-place: Timestamp field normalized)
//
// Returns:
//   - nil on success
//   - error if marshaling fails or appsrc rejects the buffer
func (r *ParticipantRecorder) pushVideoPacket(pkt *rtp.Packet) error {
	pts, relative := r.videoClockTime(pkt.Timestamp)
	pkt.Timestamp = relative

	data, err := pkt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal video RTP failed: %w", err)
	}

	buffer := gst.NewBufferFromBytes(data)
	buffer.SetPresentationTimestamp(pts)
	r.mu.Lock()
	r.videoLastPTS = pts
	r.videoLastTimestamp = relative
	r.mu.Unlock()

	if flow := r.videoAppSrc.PushBuffer(buffer); flow != gst.FlowOK {
		if flow == gst.FlowFlushing {
			return fmt.Errorf("video appsrc flushing")
		}
		return fmt.Errorf("video appsrc push failed: %s", flow.String())
	}
	return nil
}

// injectParameterSets creates a synthetic STAP-A RTP packet containing H.264 parameter sets.
//
// This function is called when recording starts but no SPS/PPS NAL units have been
// received in the RTP stream. It uses parameter sets from the SDP fmtp line
// (sprop-parameter-sets) to ensure decoders have the required codec information.
//
// Algorithm:
//  1. Validate input NAL units (non-empty, size < 64KB)
//  2. Create STAP-A payload header (0x78 = F:0, NRI:3, Type:24)
//  3. For each NAL unit:
//     - Classify as SPS or PPS
//     - Append 2-byte length prefix (big-endian)
//     - Append NAL unit bytes
//  4. Create synthetic RTP packet with modified sequence number
//  5. Push to GStreamer via pushVideoPacket()
//
// STAP-A packet format (RFC 6184 Section 5.7.1):
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|F|NRI|  Type   |         NALU 1 Size           | NALU 1 HDR    |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                         NALU 1 Data...                        |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|         NALU 2 Size           | NALU 2 HDR    | NALU 2 Data...|
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// Synthetic RTP header construction:
//   - Version: 2 (RTP version)
//   - PayloadType: Copied from reference packet (maintains stream consistency)
//   - SequenceNumber: reference.SequenceNumber - 1 (inserted before keyframe)
//   - Timestamp: Same as reference (parameter sets have no duration)
//   - SSRC: Copied from reference (maintains stream identity)
//   - Marker: false (not end of frame)
//
// Why inject before keyframe:
// Parameter sets must arrive before the IDR frame to allow decoding. By using
// sequence number (reference - 1), we ensure proper ordering in the GStreamer
// jitterbuffer and rtph264depay elements.
//
// Parameters:
//   - reference: RTP packet to use as template (typically the first keyframe)
//   - nalUnits: List of NAL units to inject (from SDP sprop-parameter-sets)
//
// Returns:
//   - spsInjected: true if at least one SPS was included
//   - ppsInjected: true if at least one PPS was included
//   - error: if no valid NAL units or pushVideoPacket fails
func (r *ParticipantRecorder) injectParameterSets(reference *rtp.Packet, nalUnits [][]byte) (bool, bool, error) {
	if len(nalUnits) == 0 {
		return false, false, fmt.Errorf("no parameter sets to inject")
	}

	payload := []byte{0x78} // F=0, NRI=3, Type=24 (STAP-A)
	var spsInjected, ppsInjected bool

	for _, nal := range nalUnits {
		if len(nal) == 0 {
			continue
		}
		if len(nal) > 0xFFFF {
			return false, false, fmt.Errorf("parameter set too large (%d bytes)", len(nal))
		}
		if sps, pps := classifyNALUnit(nal); sps || pps {
			spsInjected = spsInjected || sps
			ppsInjected = ppsInjected || pps
		}
		payload = append(payload, byte(len(nal)>>8), byte(len(nal)))
		payload = append(payload, nal...)
	}

	if len(payload) <= 1 {
		return false, false, fmt.Errorf("no valid parameter sets to inject")
	}

	packet := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    reference.PayloadType,
			SequenceNumber: reference.SequenceNumber - 1,
			Timestamp:      reference.Timestamp,
			SSRC:           reference.SSRC,
			Marker:         false,
		},
		Payload: payload,
	}

	if err := r.pushVideoPacket(packet); err != nil {
		return false, false, err
	}

	log.Printf("[%s] injected SPS/PPS via STAP-A (seq=%d)", r.logPrefix(), packet.SequenceNumber)
	return spsInjected, ppsInjected, nil
}

// videoClockTime converts an RTP timestamp to GStreamer ClockTime (nanoseconds).
//
// This function performs two critical timestamp transformations:
//  1. Normalization: Converts absolute RTP timestamp to relative (base-0)
//  2. Clock conversion: Converts from 90kHz RTP clock to nanoseconds
//
// Algorithm:
//  1. On first call: Initialize timestamp base (videoTimestampBase = ts)
//  2. Subtract base from current timestamp to get relative value
//  3. Convert from 90kHz clock to nanoseconds using formula:
//     nanoseconds = (ticks * 1_000_000_000) / 90000
//
// H.264 RTP timestamp clock (RFC 6184):
// H.264 video uses a 90kHz RTP timestamp clock, meaning each timestamp unit
// represents 1/90000 of a second (approximately 11.1 microseconds).
//
// Example conversions (30fps video, 3000 ticks per frame):
//
//	First packet:   RTP TS = 3840000000 → Base = 3840000000 → Relative = 0       → PTS = 0ns
//	Second packet:  RTP TS = 3840003000 → Base = 3840000000 → Relative = 3000    → PTS = 33333333ns (33.3ms)
//	Third packet:   RTP TS = 3840006000 → Base = 3840000000 → Relative = 6000    → PTS = 66666666ns (66.7ms)
//	Frame at 1sec:  RTP TS = 3840090000 → Base = 3840000000 → Relative = 90000   → PTS = 1000000000ns (1.0s)
//
// Timestamp wraparound handling:
// This function uses uint32 subtraction which automatically handles wraparound
// correctly due to modular arithmetic. For example:
//
//	Base = 4294960000, Current = 10000 (wrapped around)
//	Relative = uint32(10000 - 4294960000) = uint32(-4294950000) = 17296 (correct)
//
// Thread-safety:
//   - Protected by r.mu mutex
//   - videoTimestampInit and videoTimestampBase are accessed under lock
//
// Parameters:
//   - ts: RTP timestamp from video packet (32-bit, 90kHz clock)
//
// Returns:
//   - ClockTime: GStreamer presentation timestamp in nanoseconds
//   - uint32: Normalized relative timestamp (for stats/logging)
func (r *ParticipantRecorder) videoClockTime(ts uint32) (gst.ClockTime, uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.videoTimestampInit {
		r.videoTimestampInit = true
		r.videoTimestampBase = ts
	}
	relative := uint32(ts - r.videoTimestampBase)
	ptsNs := uint64(relative) * 1_000_000_000 / 90000
	return gst.ClockTime(ptsNs), relative
}

// audioClockTime converts an RTP timestamp to GStreamer ClockTime (nanoseconds).
//
// This function is the audio counterpart to videoClockTime(). It performs the same
// two transformations but uses the Opus audio clock rate (48kHz) instead of the
// H.264 video clock rate (90kHz).
//
// Algorithm:
//  1. On first call: Initialize timestamp base (audioTimestampBase = ts)
//  2. Subtract base from current timestamp to get relative value
//  3. Convert from 48kHz clock to nanoseconds using formula:
//     nanoseconds = (ticks * 1_000_000_000) / 48000
//
// Opus RTP timestamp clock (RFC 7587):
// Opus audio uses a 48kHz RTP timestamp clock, meaning each timestamp unit
// represents 1/48000 of a second (approximately 20.8 microseconds).
//
// Example conversions (20ms audio frames, 960 samples per frame):
//
//	First packet:   RTP TS = 2160000000 → Base = 2160000000 → Relative = 0       → PTS = 0ns
//	Second packet:  RTP TS = 2160000960 → Base = 2160000000 → Relative = 960     → PTS = 20000000ns (20ms)
//	Third packet:   RTP TS = 2160001920 → Base = 2160000000 → Relative = 1920    → PTS = 40000000ns (40ms)
//	Frame at 1sec:  RTP TS = 2160048000 → Base = 2160000000 → Relative = 48000   → PTS = 1000000000ns (1.0s)
//
// Why separate audio and video timestamp bases:
// Audio and video RTP streams have independent timestamp origins. For HLS muxing,
// both streams must start at PTS=0, so we normalize each stream independently.
// The muxer (mpegtsmux) synchronizes the streams based on their PTS values.
//
// Thread-safety:
//   - Protected by r.mu mutex (same mutex as video, safe because we lock)
//   - audioTimestampInit and audioTimestampBase are accessed under lock
//
// Parameters:
//   - ts: RTP timestamp from audio packet (32-bit, 48kHz clock)
//
// Returns:
//   - ClockTime: GStreamer presentation timestamp in nanoseconds
//   - uint32: Normalized relative timestamp (for stats/logging)
func (r *ParticipantRecorder) audioClockTime(ts uint32) (gst.ClockTime, uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.audioTimestampInit {
		r.audioTimestampInit = true
		r.audioTimestampBase = ts
	}
	relative := uint32(ts - r.audioTimestampBase)
	ptsNs := uint64(relative) * 1_000_000_000 / 48000
	return gst.ClockTime(ptsNs), relative
}

// signalVideoReady marks the video stream as ready and triggers callbacks.
//
// This function is called when the first video keyframe is received during the
// handshake/warm-up phase. It performs four critical state transitions:
//  1. Set videoReady flag (atomic, prevents duplicate signals)
//  2. Set handshakeReady flag (indicates SPS/PPS established)
//  3. Close videoReadyCh channel (unblocks goroutines waiting for video)
//  4. Invoke onVideoReady callback (notifies handler that recording can start)
//
// State management:
// The function uses CompareAndSwap to ensure it only executes once, even if
// called multiple times (which can happen if multiple keyframe packets arrive
// in quick succession).
//
// Handshake ready vs video ready:
//   - videoReady: Internal flag indicating first keyframe processed
//   - handshakeReady: Exported flag (via HandshakeReady()) for external checks
//   - Both are set atomically by this function
//
// Channel closure:
// Closing videoReadyCh is a Go idiom for broadcasting to multiple goroutines
// that video is ready. Any goroutine blocking on <-r.videoReadyCh will unblock.
// The sync.Once ensures the channel is closed exactly once (closing twice panics).
//
// Callback invocation:
// The onVideoReady callback (set via SetOnVideoReady) is typically used by the
// agent handler to transition from handshake publisher to recording publisher.
//
// Thread-safety:
//   - videoReady: atomic.Bool (lock-free)
//   - handshakeReady: atomic.Bool (lock-free)
//   - videoReadyOnce: sync.Once (ensures single channel close)
//   - onVideoReady callback access: protected by r.mu mutex
func (r *ParticipantRecorder) signalVideoReady() {
	if r.videoReady.CompareAndSwap(false, true) {
		r.handshakeReady.Store(true)
		r.videoReadyOnce.Do(func() {
			close(r.videoReadyCh)
		})
		var cb func()
		r.mu.Lock()
		cb = r.onVideoReady
		r.mu.Unlock()
		if cb != nil {
			cb()
		}
	}
}

// requestPLI sends a Picture Loss Indication (PLI) request to the video sender.
//
// PLI is an RTCP feedback message (RFC 4585) that requests the sender to generate
// a new keyframe (IDR frame). This is used in two scenarios:
//  1. Initial keyframe request: When track is first attached (no video received yet)
//  2. Recovery from packet loss: When waiting for keyframe during recording activation
//
// Why PLI is needed:
// WebRTC senders typically send keyframes infrequently (every few seconds) to
// reduce bandwidth. When the recorder needs a keyframe immediately (e.g., to start
// recording or recover from errors), it sends PLI to request one.
//
// PLI vs FIR:
// PLI (Picture Loss Indication) is preferred over FIR (Full Intra Request) in
// modern WebRTC implementations because it's simpler and doesn't require maintaining
// sequence numbers. Both achieve the same goal: requesting a keyframe.
//
// The writer function:
// The writer function is typically a closure over the WebRTC PeerConnection's
// WriteRTCP method. It sends the PLI RTCP packet to the video sender.
//
// Example usage in AttachVideoTrack:
//
//	if handshakeWait%200 == 0 {
//	    log.Printf("waiting for keyframe, sending PLI")
//	    r.requestPLI(pliWriter, track.SSRC())
//	}
//
// Statistics:
// This function increments videoPLIRequests counter (protected by mutex) for
// debugging and monitoring purposes.
//
// Thread-safety:
//   - videoPLIRequests: protected by r.mu mutex
//   - writer function: assumed to be thread-safe (WebRTC SDK guarantees this)
//
// Parameters:
//   - writer: Function to send PLI (typically PeerConnection.WriteRTCP wrapper)
//   - ssrc: SSRC of the video stream to request keyframe from
func (r *ParticipantRecorder) requestPLI(writer func(webrtc.SSRC), ssrc webrtc.SSRC) {
	if writer == nil {
		return
	}
	r.mu.Lock()
	r.videoPLIRequests++
	r.mu.Unlock()
	writer(ssrc)
}

// AttachVideoTrack attaches a video track for recording.
//
// It spawns a goroutine that:
//   - Requests keyframes via PLI
//   - Waits for handshake keyframe (to establish SPS/PPS)
//   - Waits for recording activation
//   - Starts pipeline on first recording keyframe
//   - Pushes RTP packets to GStreamer
//
// The pliWriter function is called to request keyframes when needed.
func (r *ParticipantRecorder) AttachVideoTrack(ctx context.Context, track *webrtc.TrackRemote, pliWriter func(webrtc.SSRC)) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		log.Printf("[%s] video track subscribed (sid=%s, codec=%s, payloadType=%d, ssrc=%d)",
			r.logPrefix(), track.ID(), track.Codec().MimeType, track.PayloadType(), track.SSRC())

		if pliWriter == nil {
			pliWriter = func(webrtc.SSRC) {}
		}

		var handshakeWait, recordingWait int
		firstPacket := true

		fmtp := track.Codec().SDPFmtpLine
		log.Printf("[%s] track fmtp: %q", r.logPrefix(), fmtp)
		spropNALs := parseSpropParameterSets(fmtp)
		if len(spropNALs) > 0 {
			log.Printf("[%s] codec fmtp provided %d parameter set(s)", r.logPrefix(), len(spropNALs))
		}
		var spsSeen, ppsSeen bool
		injectedSprop := false

		for {
			var rtpPacket *rtp.Packet
			var fromBuffer bool

			if r.recordingActive.Load() {
				if buffered := r.dequeuePreVideoPacket(); buffered != nil {
					rtpPacket = buffered
					fromBuffer = true
				}
			}

			if rtpPacket == nil {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var err error
				rtpPacket, _, err = track.ReadRTP()
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("[%s] video track read error: %v", r.logPrefix(), err)
					}
					return
				}

				// For E2EE, use frame-level decryption instead of per-packet
				// E2EE encryption happens at the frame level in Annex B format
				// We need to collect RTP packets, reassemble to Annex B, decrypt, then push raw H264
				if r.E2EEEnabled() && len(rtpPacket.Payload) > 0 {
					// Add packet to frame assembler - it will return complete Annex B frame when ready
					annexBFrame, frameTimestamp, complete := r.frameAssembler.AddPacket(rtpPacket)
					if !complete {
						// Frame not complete yet, wait for more packets
						if firstPacket {
							firstPacket = false
							r.logger.Debug("E2EE: Requesting initial keyframe via PLI")
							r.requestPLI(pliWriter, track.SSRC())
						}
						// Request PLI periodically while waiting for frames
						if !r.handshakeReady.Load() {
							handshakeWait++
							if handshakeWait%200 == 0 {
								r.logger.Debug("E2EE: Waiting for complete frame, sending PLI")
								r.requestPLI(pliWriter, track.SSRC())
							}
						}
						continue
					}

					// Frame is complete - decrypt it
					r.mu.Lock()
					e2eeCtx := r.e2eeCtx
					r.mu.Unlock()

					decryptedFrame, decryptErr := e2eeCtx.DecryptVideoFrame(annexBFrame)
					if decryptErr != nil {
						log.Printf("[%s] video E2EE frame decryption error (ts=%d, frameSize=%d): %v",
							r.logPrefix(), frameTimestamp, len(annexBFrame), decryptErr)
						continue
					}
					if decryptedFrame == nil {
						// Server Injected Frame - drop it
						continue
					}

					// Check if this decrypted frame contains a keyframe
					e2eeIsKeyframe := false
					naluIndices := findNALUIndices(decryptedFrame)
					for _, idx := range naluIndices {
						if idx < len(decryptedFrame) {
							nalType := decryptedFrame[idx] & 0x1F
							if nalType == nalUnitTypeIDR {
								e2eeIsKeyframe = true
								break
							}
						}
					}

					// Signal handshake ready on first keyframe
					if !r.handshakeReady.Load() {
						if e2eeIsKeyframe {
							r.mu.Lock()
							r.videoKeyframeCount++
							r.mu.Unlock()
							r.signalVideoReady()
							r.logger.Info("E2EE: Decrypted first keyframe (handshake ready)",
								"ts", frameTimestamp, "frameSize", len(decryptedFrame))
						} else {
							handshakeWait++
							if handshakeWait == 1 || handshakeWait%200 == 0 {
								r.logger.Debug("E2EE: Waiting for keyframe, sending PLI")
								r.requestPLI(pliWriter, track.SSRC())
							}
							continue
						}
					}

					// Wait for recording activation
					if !r.recordingActive.Load() {
						continue
					}

					// Wait for keyframe to start recording
					if r.recordingKeyframePending.Load() {
						if !e2eeIsKeyframe {
							recordingWait++
							if recordingWait == 1 || recordingWait%200 == 0 {
								r.logger.Debug("E2EE: Waiting for keyframe to begin recording, sending PLI")
								r.requestPLI(pliWriter, track.SSRC())
							}
							continue
						}

						r.mu.Lock()
						r.videoKeyframeCount++
						r.mu.Unlock()

						// Start pipeline on first recording keyframe
						if !r.pipelineStarted.Load() {
							r.logger.Info("E2EE: Starting GStreamer pipeline with first decrypted keyframe")
							if startErr := r.pipeline.SetState(gst.StatePlaying); startErr != nil {
								r.logger.Error("E2EE: start pipeline", "error", startErr)
								return
							}
							r.pipeline.DebugBinToDotFileWithTs(gst.DebugGraphShowAll, "publisher_recorder_e2ee")
							r.pipelineStarted.Store(true)
						}

						r.recordingKeyframePending.Store(false)
					}

					// Push decrypted frame to E2EE video appsrc
					if pushErr := r.pushE2EEVideoFrame(decryptedFrame, frameTimestamp); pushErr != nil {
						r.logger.Error("E2EE video push error", "error", pushErr)
						return
					}

					r.mu.Lock()
					r.videoPacketCount++
					r.videoBytesReceived += int64(len(decryptedFrame))
					r.mu.Unlock()

					continue // Skip normal RTP processing for E2EE
				}

				if firstPacket {
					firstPacket = false
					log.Printf("[%s] requesting initial keyframe via PLI (seq=%d)", r.logPrefix(), rtpPacket.SequenceNumber)
					r.requestPLI(pliWriter, track.SSRC())
				}
			}

			if rtpPacket == nil {
				continue
			}

			if len(rtpPacket.Payload) == 0 {
				if !fromBuffer {
					continue
				}
				continue
			}

			if sps, pps := detectParameterSets(rtpPacket.Payload); sps || pps {
				if sps {
					spsSeen = true
				}
				if pps {
					ppsSeen = true
				}
				log.Printf("[%s] detected parameter set packet seq=%d sps=%v pps=%v", r.logPrefix(), rtpPacket.SequenceNumber, sps, pps)
				if r.recordingActive.Load() {
					r.mu.Lock()
					if sps {
						r.videoSPSCount++
					}
					if pps {
						r.videoPPSCount++
					}
					r.mu.Unlock()
				}
			}

			isKeyframe := isH264Keyframe(rtpPacket.Payload)

			if !r.handshakeReady.Load() {
				if isKeyframe {
					if !fromBuffer {
						r.enqueuePreVideoPacket(rtpPacket)
					}
					r.mu.Lock()
					r.videoKeyframeCount++
					r.mu.Unlock()
					r.signalVideoReady()
					log.Printf("[%s] received warm-up keyframe (seq=%d ts=%d)", r.logPrefix(), rtpPacket.SequenceNumber, rtpPacket.Timestamp)
				} else {
					handshakeWait++
					if handshakeWait == 1 || handshakeWait%200 == 0 {
						log.Printf("[%s] warm-up waiting for keyframe, sending PLI (seq=%d)", r.logPrefix(), rtpPacket.SequenceNumber)
						r.requestPLI(pliWriter, track.SSRC())
					}
				}
				continue
			}

			if !r.recordingActive.Load() {
				if !fromBuffer {
					r.enqueuePreVideoPacket(rtpPacket)
				}
				continue
			}

			if r.recordingKeyframePending.Load() {
				if isKeyframe {
					if (!spsSeen || !ppsSeen) && len(spropNALs) > 0 && !injectedSprop {
						r.initVideoCaps(rtpPacket.PayloadType)
						if injectedSPS, injectedPPS, injErr := r.injectParameterSets(rtpPacket, spropNALs); injErr != nil {
							log.Printf("[%s] failed to inject codec parameter sets: %v", r.logPrefix(), injErr)
						} else {
							injectedSprop = true
							if injectedSPS {
								spsSeen = true
								r.mu.Lock()
								r.videoSPSCount++
								r.mu.Unlock()
							}
							if injectedPPS {
								ppsSeen = true
								r.mu.Lock()
								r.videoPPSCount++
								r.mu.Unlock()
							}
						}
					}
					r.mu.Lock()
					r.videoKeyframeCount++
					r.mu.Unlock()

					// Start pipeline on first recording keyframe to avoid invalid HLS segments
					if !r.pipelineStarted.Load() {
						log.Printf("[%s] starting GStreamer pipeline with first recording keyframe seq=%d ts=%d", r.logPrefix(), rtpPacket.SequenceNumber, rtpPacket.Timestamp)
						r.initVideoCaps(rtpPacket.PayloadType)
						if err := r.pipeline.SetState(gst.StatePlaying); err != nil {
							log.Printf("[%s] failed to start pipeline: %v", r.logPrefix(), err)
							return
						}
						r.pipeline.DebugBinToDotFileWithTs(gst.DebugGraphShowAll, "publisher_recorder")
						r.pipelineStarted.Store(true)
					} else {
						log.Printf("[%s] starting active recording with keyframe seq=%d ts=%d", r.logPrefix(), rtpPacket.SequenceNumber, rtpPacket.Timestamp)
					}

					// Push first video keyframe BEFORE allowing audio to start
					// This ensures first HLS segment contains both audio and video
					r.initVideoCaps(rtpPacket.PayloadType)
					if err := r.pushVideoPacket(rtpPacket); err != nil {
						log.Printf("[%s] video push error: %v", r.logPrefix(), err)
						return
					}
					r.mu.Lock()
					r.videoPacketCount++
					r.videoBytesReceived += int64(len(rtpPacket.Payload))
					r.mu.Unlock()

					// Push several more video packets from pre-buffer to prime the pipeline
					// This ensures video reaches the muxer before audio starts flooding in
					primeCount := 0
					for {
						buffered := r.dequeuePreVideoPacket()
						if buffered == nil {
							break
						}
						r.initVideoCaps(buffered.PayloadType)
						if err := r.pushVideoPacket(buffered); err != nil {
							log.Printf("[%s] video push error while priming: %v", r.logPrefix(), err)
							return
						}
						r.mu.Lock()
						r.videoPacketCount++
						r.videoBytesReceived += int64(len(buffered.Payload))
						r.mu.Unlock()
						primeCount++
					}
					log.Printf("[%s] primed pipeline with keyframe + %d video packets", r.logPrefix(), primeCount)

					// NOW allow audio to start (video pipeline is primed)
					r.recordingKeyframePending.Store(false)
					continue
				} else {
					recordingWait++
					if recordingWait == 1 || recordingWait%200 == 0 {
						log.Printf("[%s] waiting for keyframe to begin recording, sending PLI (seq=%d)", r.logPrefix(), rtpPacket.SequenceNumber)
						r.requestPLI(pliWriter, track.SSRC())
					}
					if !fromBuffer {
						r.enqueuePreVideoPacket(rtpPacket)
					}
					continue
				}
			}

			r.initVideoCaps(rtpPacket.PayloadType)
			if err := r.pushVideoPacket(rtpPacket); err != nil {
				log.Printf("[%s] video push error: %v", r.logPrefix(), err)
				return
			}

			r.mu.Lock()
			r.videoPacketCount++
			r.videoBytesReceived += int64(len(rtpPacket.Payload))
			r.mu.Unlock()
		}
	}()
}

// AttachAudioTrack attaches an audio track for recording.
//
// Audio packets are only pushed to GStreamer after recording is activated
// and the pipeline has started (triggered by the first video keyframe).
func (r *ParticipantRecorder) AttachAudioTrack(ctx context.Context, track *webrtc.TrackRemote) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		log.Printf("[%s] audio track subscribed (sid=%s, codec=%s, payloadType=%d)",
			r.logPrefix(), track.ID(), track.Codec().MimeType, track.PayloadType())
		for {
			var rtpPacket *rtp.Packet
			var fromBuffer bool

			if r.recordingActive.Load() && !r.recordingKeyframePending.Load() {
				if buffered := r.dequeuePreAudioPacket(); buffered != nil {
					rtpPacket = buffered
					fromBuffer = true
				}
			}

			if rtpPacket == nil {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var err error
				rtpPacket, _, err = track.ReadRTP()
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("[%s] audio track read error: %v", r.logPrefix(), err)
					}
					return
				}

				// Decrypt E2EE-encrypted payload if E2EE is enabled
				if r.E2EEEnabled() && len(rtpPacket.Payload) > 0 {
					r.mu.Lock()
					e2eeCtx := r.e2eeCtx
					r.mu.Unlock()

					decrypted, err := e2eeCtx.DecryptAudio(rtpPacket.Payload)
					if err != nil {
						// Decryption failed - log and skip packet
						log.Printf("[%s] audio E2EE decryption error (seq=%d): %v", r.logPrefix(), rtpPacket.SequenceNumber, err)
						continue
					}
					if decrypted == nil {
						// Server Injected Frame - drop it
						continue
					}
					rtpPacket.Payload = decrypted
				}
			}

			if rtpPacket == nil {
				continue
			}

			if !r.recordingActive.Load() || r.recordingKeyframePending.Load() {
				if !fromBuffer {
					r.enqueuePreAudioPacket(rtpPacket)
				}
				continue
			}
			r.mu.Lock()
			r.audioPacketCount++
			if len(rtpPacket.Payload) == 0 {
				r.audioEmptyPacketCount++
				r.mu.Unlock()
				continue
			}
			r.audioBytesReceived += int64(len(rtpPacket.Payload))
			if !r.audioInitialized {
				capsStr := fmt.Sprintf("application/x-rtp,media=audio,encoding-name=OPUS,clock-rate=48000,payload=%d", rtpPacket.PayloadType)
				caps := gst.NewCapsFromString(capsStr)
				_ = r.audioAppSrc.SetProperty("caps", caps)
				r.audioInitialized = true
				log.Printf("[%s] audio caps initialized: %s", r.logPrefix(), capsStr)
			}
			r.mu.Unlock()

			pts, relative := r.audioClockTime(rtpPacket.Timestamp)
			rtpPacket.Timestamp = relative

			data, err := rtpPacket.Marshal()
			if err != nil {
				log.Printf("[%s] marshal audio RTP failed: %v", r.logPrefix(), err)
				continue
			}

			buffer := gst.NewBufferFromBytes(data)
			buffer.SetPresentationTimestamp(pts)
			r.mu.Lock()
			r.audioLastPTS = pts
			r.audioLastTimestamp = relative
			r.mu.Unlock()

			if flow := r.audioAppSrc.PushBuffer(buffer); flow != gst.FlowOK {
				if flow != gst.FlowFlushing {
					log.Printf("[%s] audio appsrc push returned %s", r.logPrefix(), flow.String())
				}
				return
			}
		}
	}()
}

// VideoStreamEnded signals that the video stream has ended.
// Sends EOS to the video appsrc element.
func (r *ParticipantRecorder) VideoStreamEnded() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.videoEnded {
		return
	}
	r.videoEnded = true
	if r.videoAppSrc != nil {
		r.videoAppSrc.EndStream()
	}
}

// AudioStreamEnded signals that the audio stream has ended.
// Sends EOS to the audio appsrc element.
func (r *ParticipantRecorder) AudioStreamEnded() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.audioEnded {
		return
	}
	r.audioEnded = true
	if r.audioAppSrc != nil {
		r.audioAppSrc.EndStream()
	}
}

// Stop stops the recorder and waits for all goroutines to complete.
//
// It sends EOS to both audio and video streams, waits for the GStreamer
// pipeline to process the EOS event, and then sets the pipeline to NULL state.
// Logs detailed statistics about the recording session.
func (r *ParticipantRecorder) Stop() {
	r.stopOnce.Do(func() {
		log.Printf("[%s] stopping recorder", r.logPrefix())
		r.VideoStreamEnded()
		r.AudioStreamEnded()

		if r.pipeline != nil {
			r.pipeline.SendEvent(gst.NewEOSEvent())
			if bus := r.pipeline.GetBus(); bus != nil {
				bus.TimedPopFiltered(gst.ClockTime(5*1_000_000_000), gst.MessageEOS|gst.MessageError)
			}
			_ = r.pipeline.SetState(gst.StateNull)
		}

		r.clearPreVideoBuffer()
		r.clearPreAudioBuffer()

		if err := normalizeHLSTimestamps(r.outputDir); err != nil {
			log.Printf("[%s] failed to normalize HLS timestamps: %v", r.logPrefix(), err)
		} else {
			log.Printf("[%s] normalized HLS timestamps", r.logPrefix())
		}

		if err := fixHLSPlaylist(r.outputDir); err != nil {
			log.Printf("[%s] failed to fix HLS playlist: %v", r.logPrefix(), err)
		} else {
			log.Printf("[%s] fixed HLS playlist final segment duration", r.logPrefix())
		}

		// Close real-time S3 uploader if enabled
		if r.s3Uploader != nil {
			if err := r.s3Uploader.Close(); err != nil {
				log.Printf("[%s] failed to close S3 uploader: %v", r.logPrefix(), err)
			}
		}

		videoSeconds := float64(r.videoLastPTS) / 1_000_000_000
		audioSeconds := float64(r.audioLastPTS) / 1_000_000_000
		log.Printf("[%s] recorder stats: videoPackets=%d emptyVideo=%d videoBytes=%d keyframes=%d sps=%d pps=%d plis=%d audioPackets=%d emptyAudio=%d audioBytes=%d videoPTS=%.3fs audioPTS=%.3fs videoBase=%d audioBase=%d videoLastTS=%d audioLastTS=%d",
			r.logPrefix(),
			r.videoPacketCount,
			r.videoEmptyPacketCount,
			r.videoBytesReceived,
			r.videoKeyframeCount,
			r.videoSPSCount,
			r.videoPPSCount,
			r.videoPLIRequests,
			r.audioPacketCount,
			r.audioEmptyPacketCount,
			r.audioBytesReceived,
			videoSeconds,
			audioSeconds,
			r.videoTimestampBase,
			r.audioTimestampBase,
			r.videoLastTimestamp,
			r.audioLastTimestamp)
	})
	r.wg.Wait()
}

// OutputDirectory returns the absolute path to the directory containing
// the recording files (output.ts, playlist.m3u8, segment*.ts).
func (r *ParticipantRecorder) OutputDirectory() string {
	return r.outputDir
}

// Summary returns a summary of the recording session including
// file size, duration, and packet counts.
func (r *ParticipantRecorder) Summary() RecordingSummary {
	summary := RecordingSummary{
		Participant: r.participant,
		Room:        r.room,
	}

	outputFile := filepath.Join(r.outputDir, "output.ts")
	summary.OutputFile = outputFile

	stat, err := os.Stat(outputFile)
	if err != nil {
		summary.Err = fmt.Errorf("failed to stat recording: %w", err)
		return summary
	}

	summary.SizeBytes = stat.Size()
	summary.Duration = time.Since(r.startTime)
	summary.VideoPackets = r.videoPacketCount
	summary.AudioPackets = r.audioPacketCount

	return summary
}
