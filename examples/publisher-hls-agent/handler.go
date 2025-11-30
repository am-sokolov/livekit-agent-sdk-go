package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/am-sokolov/livekit-agent-sdk-go/pkg/agent"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
)

// PublisherHLSHandler handles LiveKit JT_PUBLISHER jobs for HLS recording.
//
// It implements the agent.Handler interface and manages recording sessions
// for target participants. Each session creates a GStreamer pipeline that
// generates HLS playlists and segments from RTP media streams.
//
// The handler supports:
//   - Auto-activation: Automatically start recording when tracks are ready
//   - Manual control: Programmatic recording activation via ActivateRecording
//   - Multi-session: Handle multiple concurrent recording jobs
//   - S3 upload: Optional upload of completed recordings to S3
//
// Example usage:
//
//	cfg := loadConfig()
//	handler := NewPublisherHLSHandler(cfg)
//
//	// Wait for first track subscription
//	if err := handler.WaitReady(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Manually activate recording if AUTO_ACTIVATE_RECORDING=false
//	if err := handler.ActivateRecording("participant-identity"); err != nil {
//	    log.Fatal(err)
//	}
type PublisherHLSHandler struct {
	agent.BaseHandler
	cfg *Config

	mu                  sync.Mutex
	sessions            map[string]*recordingSession
	summaries           []RecordingSummary
	participantSessions map[string]*recordingSession
	readyOnce           sync.Once
	readyCh             chan struct{}
}

// WaitReady blocks until the handler has successfully subscribed to at least one track,
// or until the context is cancelled. This is useful for synchronizing recording
// activation in manual mode.
//
// Returns nil when ready, or ctx.Err() if the context is cancelled.
func (h *PublisherHLSHandler) WaitReady(ctx context.Context) error {
	select {
	case <-h.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recordingSession represents an active recording session for a single participant.
// Each session maintains its own GStreamer pipeline, track subscriptions, and lifecycle.
//
// The session tracks:
//   - Recorder: The GStreamer pipeline managing HLS generation
//   - Track readiness: Whether audio and video tracks have been subscribed
//   - Activation state: Whether recording has been activated (manually or automatically)
//   - Cancellation: Context cancellation for clean shutdown
//
// Thread-safety: All fields except cancel are protected by mu.
type recordingSession struct {
	mu          sync.Mutex                   // Protects tracksReady and activated
	cancel      context.CancelFunc           // Cancels the session context, stopping recording
	recorder    *ParticipantRecorder         // GStreamer pipeline for this participant
	participant string                       // Participant identity being recorded
	tracksReady map[webrtc.RTPCodecType]bool // Tracks which media types have been subscribed
	activated   bool                         // Whether ActivateRecording has been called
}

// NewPublisherHLSHandler creates a new handler for JT_PUBLISHER jobs.
//
// The handler will use the provided configuration for all recording sessions,
// including output directory, S3 upload settings, and auto-activation behavior.
func NewPublisherHLSHandler(cfg *Config) *PublisherHLSHandler {
	return &PublisherHLSHandler{
		cfg:                 cfg,
		sessions:            make(map[string]*recordingSession),
		participantSessions: make(map[string]*recordingSession),
		readyCh:             make(chan struct{}),
	}
}

// OnJobRequest is called when the LiveKit server dispatches a job to this agent.
// This method determines whether the agent should accept or reject the job.
//
// The handler only accepts JT_PUBLISHER jobs that include valid participant information.
// Each accepted job will trigger OnJobAssigned, creating a new recording session.
//
// Returns:
//   - bool: true to accept the job, false to reject
//   - *agent.JobMetadata: Metadata for the agent's participant in the room
//
// The returned JobMetadata configures how the agent appears in the LiveKit room:
//   - ParticipantIdentity: Empty string to let LiveKit assign a unique identity
//   - ParticipantName: Display name shown in the room participant list
//   - ParticipantMetadata: JSON metadata for client identification
func (h *PublisherHLSHandler) OnJobRequest(ctx context.Context, job *livekit.Job) (bool, *agent.JobMetadata) {
	_ = ctx // Context parameter required by interface but not used
	if job.Type != livekit.JobType_JT_PUBLISHER {
		log.Printf("rejecting job %s: unsupported type %s", job.Id, job.Type.String())
		return false, nil
	}

	if job.Participant == nil || job.Participant.Identity == "" {
		log.Printf("rejecting job %s: missing participant information", job.Id)
		return false, nil
	}

	log.Printf("accepting JT_PUBLISHER job %s for participant %s in room %s", job.Id, job.Participant.Identity, job.Room.Name)

	return true, &agent.JobMetadata{
		ParticipantIdentity: "",
		ParticipantName:     "publisher-hls-recorder",
		ParticipantMetadata: `{"agent":"publisher-hls-recorder"}`,
	}
}

// OnJobAssigned is called after OnJobRequest accepts a job. This method sets up and executes
// the recording session for the target participant.
//
// The method performs the following steps:
//  1. Creates a ParticipantRecorder with a GStreamer pipeline
//  2. Connects to the LiveKit room as a recorder participant
//  3. Subscribes to the target participant's audio and video tracks
//  4. Attaches tracks to the GStreamer pipeline for HLS generation
//  5. Manages auto-activation if configured
//  6. Handles cleanup and S3 upload on completion
//
// Recording lifecycle:
//   - Pipeline starts when the first recording keyframe arrives
//   - Recording activates either automatically (if AUTO_ACTIVATE_RECORDING=true)
//     or manually via ActivateRecording()
//   - Session ends when the participant disconnects, context is cancelled,
//     or the server terminates the job
//
// Cleanup behavior:
//   - If S3 upload succeeds, the local recording directory is removed
//   - If recording never started (early failure/cancellation), directory is removed
//   - Otherwise, the local recording is preserved for manual inspection
//
// Returns an error if recorder creation or room connection fails.
func (h *PublisherHLSHandler) OnJobAssigned(ctx context.Context, jobCtx *agent.JobContext) error {
	if jobCtx.Job.Participant == nil || jobCtx.Job.Participant.Identity == "" {
		return fmt.Errorf("job %s missing participant identity", jobCtx.Job.Id)
	}

	participantIdentity := jobCtx.Job.Participant.Identity
	roomName := jobCtx.Job.Room.Name

	recorder, err := NewParticipantRecorder(h.cfg, roomName, participantIdentity, h.cfg.E2EEEnabled())
	if err != nil {
		return fmt.Errorf("failed to create recorder: %w", err)
	}

	if state := jobCtx.Job.GetState(); state != nil {
		log.Printf("[debug] job state: participantIdentity=%s status=%s", state.GetParticipantIdentity(), state.GetStatus().String())
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	session := &recordingSession{
		cancel:      cancel,
		recorder:    recorder,
		participant: participantIdentity,
		tracksReady: make(map[webrtc.RTPCodecType]bool),
	}

	recorder.SetOnVideoReady(func() {
		h.notifyReady()
		h.tryAutoActivate(session)
	})

	h.storeSession(jobCtx.Job.Id, participantIdentity, session)

	started := false
	defer func() {
		cancel()
		recorder.Stop()
		h.removeSession(jobCtx.Job.Id)

		outputDir := recorder.OutputDirectory()
		cleanupDir := false

		if started {
			summary := recorder.Summary()
			// Handle S3 upload based on upload mode
			if h.cfg.S3.Enabled() {
				if h.cfg.S3RealTimeUpload {
					// Real-time upload: files already uploaded during recording
					// Get S3 URL from the recorder's uploader
					if recorder.s3Uploader != nil {
						summary.Remote = recorder.s3Uploader.GetS3URL()
						log.Printf("[%s/%s] uploaded recording to %s (real-time S3)", roomName, participantIdentity, summary.Remote)
						cleanupDir = true // S3 upload successful, safe to clean up
					}
				} else {
					// Post-processing upload: upload all files after recording completes
					ctx, uploadCancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer uploadCancel()
					if remote, err := uploadRecordingToS3(ctx, h.cfg.S3, roomName, participantIdentity, recorder.OutputDirectory()); err != nil {
						log.Printf("[%s/%s] failed to upload recording to S3: %v", roomName, participantIdentity, err)
						if summary.Err == nil {
							summary.Err = err
						}
					} else {
						summary.Remote = remote
						log.Printf("[%s/%s] uploaded recording to %s", roomName, participantIdentity, remote)
						cleanupDir = true // S3 upload successful, safe to clean up
					}
				}
			}
			h.addSummary(summary)
		} else {
			// Recording was stopped before it started (e.g., cancellation, early failure)
			cleanupDir = true
		}

		// Clean up temporary directory
		if cleanupDir && outputDir != "" {
			if err := os.RemoveAll(outputDir); err != nil {
				log.Printf("[%s/%s] warning: failed to remove temporary directory %s: %v", roomName, participantIdentity, outputDir, err)
			} else {
				log.Printf("[%s/%s] cleaned up temporary directory: %s", roomName, participantIdentity, outputDir)
			}
		}
	}()

	if err := recorder.Start(); err != nil {
		return fmt.Errorf("failed to start recorder: %w", err)
	}
	started = true

	trackSet := &trackRegistry{seen: make(map[string]struct{})}
	targetIdentity := participantIdentity

	roomCallback := lksdk.NewRoomCallback()
	roomCallback.OnParticipantDisconnected = func(rp *lksdk.RemoteParticipant) {
		if rp.Identity() == targetIdentity {
			log.Printf("[%s/%s] participant disconnected, stopping recording", roomName, targetIdentity)
			cancel()
		}
	}
	roomCallback.OnDisconnected = func() {
		log.Printf("[%s/%s] recorder connection disconnected by server", roomName, targetIdentity)
		cancel()
	}
	roomCallback.ParticipantCallback.OnTrackPublished = func(publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if rp.Identity() != targetIdentity {
			return
		}
		if err := publication.SetSubscribed(true); err != nil {
			log.Printf("[%s/%s] failed to subscribe to track %s: %v", roomName, targetIdentity, publication.SID(), err)
		} else {
			log.Printf("[%s/%s] requested subscription to track %s", roomName, targetIdentity, publication.SID())
		}
		if publication.Kind() == lksdk.TrackKindVideo {
			publication.SetEnabled(true)
			if err := publication.SetVideoQuality(livekit.VideoQuality_HIGH); err != nil {
				log.Printf("[%s/%s] failed to set video quality for %s: %v", roomName, targetIdentity, publication.SID(), err)
			} else {
				log.Printf("[%s/%s] requested HIGH quality for track %s", roomName, targetIdentity, publication.SID())
			}
		}
	}
	roomCallback.ParticipantCallback.OnTrackSubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if rp.Identity() != targetIdentity {
			return
		}
		if !trackSet.mark(publication.SID()) {
			return
		}
		// Track is already subscribed at this point - no need to call SetSubscribed(true) again
		if publication.Kind() == lksdk.TrackKindVideo {
			if info := publication.TrackInfo(); info != nil {
				log.Printf("[%s/%s] track info: %s", roomName, targetIdentity, info.String())
			}
			if receiver := publication.Receiver(); receiver != nil {
				if params := receiver.GetParameters(); params.Codecs != nil {
					for _, codec := range params.Codecs {
						log.Printf("[%s/%s] receiver codec: mime=%s fmtp=%s", roomName, targetIdentity, codec.MimeType, codec.SDPFmtpLine)
					}
				}
			}
		}
		log.Printf("[%s/%s] track subscribed: sid=%s kind=%s codec=%s payloadType=%d", roomName, targetIdentity, publication.SID(), publication.Kind(), track.Codec().MimeType, track.PayloadType())
		switch track.Kind() {
		case webrtc.RTPCodecTypeVideo:
			recorder.AttachVideoTrack(sessionCtx, track, rp.WritePLI)
			if session, ok := h.getSessionByParticipant(targetIdentity); ok {
				session.setTrackReady(webrtc.RTPCodecTypeVideo)
				h.tryAutoActivate(session)
			}
		case webrtc.RTPCodecTypeAudio:
			recorder.AttachAudioTrack(sessionCtx, track)
			if session, ok := h.getSessionByParticipant(targetIdentity); ok {
				session.setTrackReady(webrtc.RTPCodecTypeAudio)
				h.tryAutoActivate(session)
			}
		default:
			log.Printf("[%s/%s] unsupported track kind %s", roomName, targetIdentity, track.Kind().String())
		}
	}
	roomCallback.ParticipantCallback.OnTrackUnsubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if rp.Identity() != targetIdentity {
			return
		}
		trackSet.unmark(publication.SID())
		switch track.Kind() {
		case webrtc.RTPCodecTypeVideo:
			recorder.VideoStreamEnded()
		case webrtc.RTPCodecTypeAudio:
			recorder.AudioStreamEnded()
		default:
			log.Printf("[%s/%s] unsubscribed from unsupported track kind %s", roomName, targetIdentity, track.Kind().String())
		}
	}

	recorderIdentity := jobCtx.Job.State.GetParticipantIdentity()
	if recorderIdentity == "" {
		recorderIdentity = fmt.Sprintf("recorder-%s", jobCtx.Job.Id)
	}

	connectInfo := lksdk.ConnectInfo{
		APIKey:              h.cfg.APIKey,
		APISecret:           h.cfg.APISecret,
		RoomName:            roomName,
		ParticipantIdentity: recorderIdentity,
		ParticipantName:     "HLS Recorder",
		ParticipantMetadata: fmt.Sprintf(`{"agent":"%s"}`, h.cfg.AgentName),
	}

	directRoom, err := lksdk.ConnectToRoom(h.cfg.LiveKitURL, connectInfo, roomCallback, lksdk.WithAutoSubscribe(false))
	if err != nil {
		return fmt.Errorf("failed to connect to room %s: %w", roomName, err)
	}
	defer directRoom.Disconnect()

	// Initialize E2EE context if enabled
	if h.cfg.E2EEEnabled() {
		e2eeCtx, err := NewE2EEContext(h.cfg.E2EEPassphrase, directRoom.SifTrailer())
		if err != nil {
			return fmt.Errorf("failed to initialize E2EE context: %w", err)
		}
		recorder.SetE2EEContext(e2eeCtx)
		log.Printf("[%s/%s] E2EE decryption enabled", roomName, targetIdentity)
	}

	if rp := directRoom.GetParticipantByIdentity(targetIdentity); rp != nil {
		for _, pub := range rp.TrackPublications() {
			if remotePub, ok := pub.(*lksdk.RemoteTrackPublication); ok {
				if err := remotePub.SetSubscribed(true); err != nil {
					log.Printf("[%s/%s] failed to subscribe to existing track %s: %v", roomName, targetIdentity, remotePub.SID(), err)
				}
				if remotePub.Kind() == lksdk.TrackKindVideo {
					remotePub.SetEnabled(true)
					if err := remotePub.SetVideoQuality(livekit.VideoQuality_HIGH); err != nil {
						log.Printf("[%s/%s] failed to set video quality for existing track %s: %v", roomName, targetIdentity, remotePub.SID(), err)
					} else {
						log.Printf("[%s/%s] requested HIGH quality for existing track %s", roomName, targetIdentity, remotePub.SID())
					}
				}
			}
		}
	} else {
		log.Printf("[%s/%s] waiting for participant tracks", roomName, targetIdentity)
	}

	select {
	case <-sessionCtx.Done():
	case <-ctx.Done():
	}

	return nil
}

// OnJobTerminated is called when the LiveKit server terminates a job.
// This can occur due to:
//   - Explicit job termination via the LiveKit API
//   - Server-side timeout or resource constraints
//   - Room deletion while the job is active
//
// The method cancels the recording session's context, triggering cleanup
// in OnJobAssigned's defer block. This ensures proper shutdown of the
// GStreamer pipeline, S3 upload (if configured), and removal of the session
// from the handler's tracking maps.
func (h *PublisherHLSHandler) OnJobTerminated(ctx context.Context, jobID string) {
	_ = ctx // Context parameter required by interface but not used
	h.mu.Lock()
	session, ok := h.sessions[jobID]
	h.mu.Unlock()

	if !ok {
		return
	}

	log.Printf("job %s terminated by server", jobID)
	session.cancel()
}

// PrintSummary logs a summary of all completed recordings in this session.
// Called on shutdown to provide recording statistics.
func (h *PublisherHLSHandler) PrintSummary() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.summaries) == 0 {
		log.Println("no recordings completed in this session")
		return
	}

	log.Println("=== Publisher HLS Recording Summary ===")
	for _, summary := range h.summaries {
		if summary.Err != nil {
			log.Printf("• %s in room %s → error: %v", summary.Participant, summary.Room, summary.Err)
			continue
		}
		logLine := fmt.Sprintf("• %s in room %s → file %s (%.2f MB), captured for %.1fs, packets video=%d audio=%d",
			summary.Participant, summary.Room, summary.OutputFile,
			float64(summary.SizeBytes)/1_000_000, summary.Duration.Seconds(),
			summary.VideoPackets, summary.AudioPackets)
		if summary.Remote != "" {
			logLine = logLine + fmt.Sprintf(" uploaded to %s", summary.Remote)
		}
		log.Println(logLine)
	}
}

// storeSession registers a new recording session in the handler's tracking maps.
// Sessions are indexed both by job ID (for OnJobTerminated) and participant identity
// (for ActivateRecording and auto-activation).
//
// Thread-safe: Protected by h.mu.
func (h *PublisherHLSHandler) storeSession(jobID, participant string, session *recordingSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[jobID] = session
	h.participantSessions[participant] = session
}

// removeSession unregisters a recording session from the handler's tracking maps.
// Called during cleanup in OnJobAssigned's defer block to ensure sessions are
// properly removed regardless of how the recording ended.
//
// Thread-safe: Protected by h.mu.
func (h *PublisherHLSHandler) removeSession(jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	session, ok := h.sessions[jobID]
	if ok {
		delete(h.participantSessions, session.participant)
	}
	delete(h.sessions, jobID)
}

// addSummary appends a recording summary to the handler's summary list.
// Summaries are collected throughout the session and printed via PrintSummary()
// on shutdown to provide a consolidated view of all recordings.
//
// Thread-safe: Protected by h.mu.
func (h *PublisherHLSHandler) addSummary(summary RecordingSummary) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.summaries = append(h.summaries, summary)
}

// getSessionByParticipant retrieves the active recording session for a participant.
// Used by ActivateRecording() to enable manual recording control and by auto-activation
// to check session readiness.
//
// Returns:
//   - *recordingSession: The session for this participant
//   - bool: true if a session exists, false otherwise
//
// Thread-safe: Protected by h.mu.
func (h *PublisherHLSHandler) getSessionByParticipant(participant string) (*recordingSession, bool) {
	h.mu.Lock()
	session, ok := h.participantSessions[participant]
	h.mu.Unlock()
	return session, ok
}

// notifyReady signals that the handler is ready for operation by closing readyCh.
// This occurs when the first video track subscription is acknowledged, indicating
// that the GStreamer pipeline has successfully connected to LiveKit media streams.
//
// The ready signal is used by WaitReady() to block until the handler is operational.
// Uses sync.Once to ensure the channel is only closed once, preventing panics on
// subsequent calls.
func (h *PublisherHLSHandler) notifyReady() {
	h.readyOnce.Do(func() {
		log.Println("handler ready: video track subscription acknowledged")
		close(h.readyCh)
	})
}

// ActivateRecording manually activates recording for the specified participant.
//
// This is used when AUTO_ACTIVATE_RECORDING=false to programmatically control
// when recording begins. Recording will start at the next keyframe after activation.
//
// Returns an error if no active session exists for the participant.
func (h *PublisherHLSHandler) ActivateRecording(participant string) error {
	session, ok := h.getSessionByParticipant(participant)
	if !ok {
		return fmt.Errorf("no active session for participant %s", participant)
	}
	log.Printf("activating recording for participant %s", participant)
	session.recorder.ActivateRecording()
	return nil
}

// tryAutoActivate attempts to automatically activate recording for a session
// when AUTO_ACTIVATE_RECORDING=true. This is called whenever the session state
// changes (track subscription, video handshake completion).
//
// Recording auto-activates when all conditions are met:
//   - AUTO_ACTIVATE_RECORDING=true in configuration
//   - Video handshake completed (SPS/PPS received, pipeline primed)
//   - Both audio and video tracks are subscribed
//   - Session has not already been activated
//
// The function uses markActivatedIfReady() to atomically check and update
// activation status, preventing duplicate activations.
func (h *PublisherHLSHandler) tryAutoActivate(session *recordingSession) {
	if !h.cfg.AutoActivate {
		return
	}

	if !session.recorder.HandshakeReady() {
		return
	}

	if !session.markActivatedIfReady() {
		return
	}

	log.Printf("[%s/%s] auto-activating recording", session.recorder.room, session.participant)
	session.recorder.ActivateRecording()
}

// setTrackReady marks a media type (audio or video) as subscribed and ready.
// Called from OnTrackSubscribed callbacks when the GStreamer pipeline successfully
// attaches a track. Auto-activation checks this state to ensure both tracks are
// ready before starting recording.
//
// Thread-safe: Protected by s.mu.
func (s *recordingSession) setTrackReady(kind webrtc.RTPCodecType) {
	s.mu.Lock()
	if s.tracksReady == nil {
		s.tracksReady = make(map[webrtc.RTPCodecType]bool)
	}
	s.tracksReady[kind] = true
	s.mu.Unlock()
}

// markActivatedIfReady atomically checks if the session is ready for activation
// and marks it as activated if all conditions are met.
//
// Activation requires:
//   - Session not already activated
//   - Both audio and video tracks subscribed (tracksReady[Video] && tracksReady[Audio])
//   - Video handshake completed (recorder.HandshakeReady())
//
// Returns:
//   - true: Session was not activated and all conditions met, now marked activated
//   - false: Session already activated or conditions not met
//
// Thread-safe: Protected by s.mu to prevent race conditions during concurrent
// track subscriptions or handshake completion.
func (s *recordingSession) markActivatedIfReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activated {
		return false
	}
	if !s.tracksReady[webrtc.RTPCodecTypeVideo] || !s.tracksReady[webrtc.RTPCodecTypeAudio] {
		return false
	}
	if !s.recorder.HandshakeReady() {
		return false
	}
	s.activated = true
	return true
}

// trackRegistry tracks which tracks have been subscribed to prevent duplicate
// handling of OnTrackSubscribed callbacks. LiveKit may fire subscription callbacks
// multiple times for the same track due to reconnections or SDP renegotiation.
//
// The registry uses track SIDs (server-assigned identifiers) as keys, ensuring
// that AttachVideoTrack/AttachAudioTrack are called exactly once per track.
//
// Thread-safety: All operations are protected by mu.
type trackRegistry struct {
	mu   sync.Mutex          // Protects seen map
	seen map[string]struct{} // Set of track SIDs already subscribed
}

// mark attempts to mark a track SID as seen. Returns true if this is the first time
// seeing this SID, false if the track was already marked (duplicate subscription).
//
// This prevents duplicate AttachVideoTrack/AttachAudioTrack calls when LiveKit
// fires OnTrackSubscribed multiple times for the same track.
//
// Thread-safe: Protected by r.mu.
func (r *trackRegistry) mark(sid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[sid]; ok {
		return false
	}
	r.seen[sid] = struct{}{}
	return true
}

// unmark removes a track SID from the registry. Called when a track is unsubscribed
// to allow potential re-subscription if the track is published again.
//
// Thread-safe: Protected by r.mu.
func (r *trackRegistry) unmark(sid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, sid)
}
