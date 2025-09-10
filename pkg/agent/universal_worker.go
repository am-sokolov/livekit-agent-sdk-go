// Package agent provides a framework for building LiveKit agents.
package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)

// UniversalWorker is a unified agent worker that can handle all job types
// and provides all capabilities in a single, flexible interface.
//
// This is the recommended way to build agents. The older Worker, ParticipantAgent,
// and PublisherAgent classes are deprecated.
//
// Key features:
//   - Handles all job types (ROOM, PARTICIPANT, PUBLISHER)
//   - Media handling capabilities available to all workers
//   - Participant management and tracking
//   - Automatic event routing based on job type
//   - Clean, simple API with optional features
type UniversalWorker struct {
	// Core fields
	serverURL string
	apiKey    string
	apiSecret string
	opts      WorkerOptions
	handler   UniversalHandler

	// Connection management
	mu            sync.RWMutex
	conn          *websocket.Conn
	workerID      string
	status        WorkerStatus
	activeJobs    map[string]*JobContext
	jobStartTimes map[string]time.Time

	// WebSocket state
	wsState       WebSocketState
	reconnectChan chan struct{}

	// Shared capabilities
	rooms               map[string]*lksdk.Room
	participantTrackers map[string]*ParticipantTracker // Per room
	eventProcessor      *UniversalEventProcessor
	loadCalculator      LoadCalculator

	// Status management
	statusQueue     []statusUpdate
	statusQueueChan chan struct{}
	loadBatcher     *LoadBatcher

	// Lifecycle
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// Logging
	logger Logger

	// Recovery and state management
	savedState        *WorkerState
	recoveryManager   *JobRecoveryManager
	timingManager     *JobTimingManager
	raceProtector     *StatusUpdateRaceProtector
	jobCheckpoints    map[string]*JobCheckpoint
	jobCheckpointsMu  sync.RWMutex
	networkHandler    *NetworkHandler
	shutdownHandler   *ShutdownHandler
	metricsCollector  *SystemMetricsCollector
	resourceMonitor   *ResourceMonitor
	resourceLimiter   *ResourceLimiter
	messageHandler    *MessageHandler
	protocolValidator *ProtocolValidator

	// Job queue and resource pool
	jobQueue     *JobQueue
	resourcePool *ResourcePool

	// Metrics tracking
	metrics struct {
		jobsAccepted  int64
		jobsRejected  int64
		jobsCompleted int64
		jobsFailed    int64
	}

	// Health tracking
	healthCheck struct {
		lastPing    time.Time
		lastPong    time.Time
		missedPings int
		isHealthy   bool
	}

	// Publisher agent functionality
	subscribedTracks    map[string]*PublisherTrackSubscription
	qualityController   *QualityController
	connectionMonitor   *ConnectionQualityMonitor
	subscriptionManager *TrackSubscriptionManager

	// Participant agent functionality
	targetParticipant   *lksdk.RemoteParticipant
	participants        map[string]*ParticipantInfo
	permissionManager   *ParticipantPermissionManager
	coordinationManager *MultiParticipantCoordinator

	// Additional fields for testing
	debugMode              bool
	consecutiveFailures    int
	circuitBreakerOpenedAt time.Time
	rateLimitMu            sync.Mutex
	lastRequestTime        time.Time
}

// JobContext contains all context for an active job
type JobContext struct {
	Job       *livekit.Job
	Room      *lksdk.Room
	Cancel    context.CancelFunc
	StartedAt time.Time

	// Job-specific context
	TargetParticipant *lksdk.RemoteParticipant // For PARTICIPANT jobs
	PublisherInfo     *PublisherInfo           // For PUBLISHER jobs
	CustomData        map[string]interface{}   // For extension data
}

// PublisherInfo contains information about a publisher
type PublisherInfo struct {
	Identity    string
	Name        string
	Metadata    string
	Permissions *livekit.ParticipantPermission
}

// NewUniversalWorker creates a worker that can handle any job type
func NewUniversalWorker(serverURL, apiKey, apiSecret string, handler UniversalHandler, opts WorkerOptions) *UniversalWorker {
	if opts.PingInterval == 0 {
		opts.PingInterval = defaultPingInterval
	}
	if opts.PingTimeout == 0 {
		opts.PingTimeout = defaultPingTimeout
	}
	if opts.Logger == nil {
		// Use default logger wrapper
		opts.Logger = NewDefaultLogger()
	}

	w := &UniversalWorker{
		serverURL:           serverURL,
		apiKey:              apiKey,
		apiSecret:           apiSecret,
		opts:                opts,
		handler:             handler,
		activeJobs:          make(map[string]*JobContext),
		jobStartTimes:       make(map[string]time.Time),
		rooms:               make(map[string]*lksdk.Room),
		participantTrackers: make(map[string]*ParticipantTracker),
		status:              WorkerStatusAvailable,
		reconnectChan:       make(chan struct{}, 1),
		statusQueue:         make([]statusUpdate, 0),
		statusQueueChan:     make(chan struct{}, 100),
		jobCheckpoints:      make(map[string]*JobCheckpoint),
		stopCh:              make(chan struct{}),
		doneCh:              make(chan struct{}),
		logger:              opts.Logger,
		subscribedTracks:    make(map[string]*PublisherTrackSubscription),
		participants:        make(map[string]*ParticipantInfo),
	}

	// Initialize state for recovery
	w.savedState = &WorkerState{
		ActiveJobs: make(map[string]*JobState),
	}

	// Initialize health check
	w.healthCheck.isHealthy = true

	// Initialize load calculator
	if opts.LoadCalculator != nil {
		w.loadCalculator = opts.LoadCalculator
	} else if opts.EnableCPUMemoryLoad {
		w.loadCalculator = NewCPUMemoryLoadCalculator()
		w.metricsCollector = NewSystemMetricsCollector()
	} else {
		w.loadCalculator = &DefaultLoadCalculator{}
	}

	// Initialize components
	w.eventProcessor = NewUniversalEventProcessor()
	w.timingManager = NewJobTimingManager()
	w.raceProtector = NewStatusUpdateRaceProtector()
	w.networkHandler = NewNetworkHandler()
	w.shutdownHandler = NewShutdownHandler(opts.Logger)
	w.messageHandler = NewMessageHandler(opts.Logger)

	// Initialize job queue if enabled
	if opts.EnableJobQueue {
		queueOpts := JobQueueOptions{
			MaxSize: opts.JobQueueSize,
		}
		w.jobQueue = NewJobQueue(queueOpts)
	}

	// Initialize resource pool if enabled
	if opts.EnableResourcePool && opts.ResourceFactory != nil {
		poolOpts := ResourcePoolOptions{
			MinSize: opts.ResourcePoolMinSize,
			MaxSize: opts.ResourcePoolMaxSize,
		}
		pool, err := NewResourcePool(opts.ResourceFactory, poolOpts)
		if err == nil {
			w.resourcePool = pool
		}
	}

	// Initialize publisher agent components
	w.qualityController = NewQualityController()
	w.connectionMonitor = NewConnectionQualityMonitor()
	w.subscriptionManager = NewTrackSubscriptionManager()

	// Initialize participant agent components
	w.permissionManager = NewParticipantPermissionManager()
	w.coordinationManager = NewMultiParticipantCoordinator()

	// Initialize protocol validator
	if opts.StrictProtocolMode {
		w.protocolValidator = NewProtocolValidator(true)
	} else {
		w.protocolValidator = NewProtocolValidator(false)
	}

	// Set up custom message handlers
	for _, handler := range opts.CustomMessageHandlers {
		w.messageHandler.RegisterHandler(handler.GetMessageType(), func(msg *ServerMessage) error {
			// Convert ServerMessage to bytes for the handler
			// In a real implementation, this would marshal the message
			return handler.HandleMessage(context.Background(), nil)
		})
	}

	// Initialize recovery if enabled
	if opts.EnableJobRecovery {
		recoveryHandler := opts.JobRecoveryHandler
		if recoveryHandler == nil {
			recoveryHandler = &DefaultJobRecoveryHandler{}
		}
		// JobRecoveryManager now uses WorkerInterface
		w.recoveryManager = NewJobRecoveryManager(w, recoveryHandler)
	}

	// Initialize resource monitoring
	if opts.EnableResourceMonitoring {
		memLimit := opts.MemoryLimitMB
		if memLimit == 0 {
			memLimit = 1024 // Default 1GB
		}
		goroutineLimit := opts.GoroutineLimit
		if goroutineLimit == 0 {
			goroutineLimit = 10000
		}
		// TODO: ResourceMonitor requires zap.Logger, not our Logger interface
		// For now, resource monitoring is disabled for UniversalWorker
		// w.resourceMonitor = NewResourceMonitor(nil, ResourceMonitorOptions{
		//     MemoryLimitMB:  memLimit,
		//     GoroutineLimit: goroutineLimit,
		// })
	}

	// Initialize resource limits
	if opts.EnableResourceLimits {
		// Create a zap logger for resource limiter
		zapLogger, _ := zap.NewProduction()
		w.resourceLimiter = NewResourceLimiter(zapLogger, ResourceLimiterOptions{
			MemoryLimitMB:      opts.HardMemoryLimitMB,
			CPUQuotaPercent:    opts.CPUQuotaPercent,
			MaxFileDescriptors: opts.MaxFileDescriptors,
			CheckInterval:      1 * time.Second,
		})

		// Start the resource limiter
		if w.resourceLimiter != nil {
			ctx := context.Background()
			w.resourceLimiter.Start(ctx)
		}
	}

	// Initialize load batching
	if opts.StatusUpdateBatchInterval > 0 {
		// LoadBatcher now uses WorkerInterface
		w.loadBatcher = NewLoadBatcher(w, opts.StatusUpdateBatchInterval)
	}

	return w
}

// Start begins the worker's operation
func (w *UniversalWorker) Start(ctx context.Context) error {
	if err := w.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Start background tasks
	go w.handleMessages(ctx)
	go w.maintainConnection(ctx)
	go w.handleStatusUpdateRetries(ctx)

	// Send initial load update after registration
	w.updateLoad()

	if w.resourceMonitor != nil {
		go w.resourceMonitor.Start(ctx)
	}

	// Wait for context cancellation or stop
	select {
	case <-ctx.Done():
		return w.Stop()
	case <-w.stopCh:
		return nil
	}
}

// Stop gracefully shuts down the worker
func (w *UniversalWorker) Stop() error {
	var err error
	w.stopOnce.Do(func() {
		close(w.stopCh)

		// Stop all active jobs
		w.mu.Lock()
		for jobID, jobCtx := range w.activeJobs {
			if jobCtx.Cancel != nil {
				jobCtx.Cancel()
			}
			w.handler.OnJobTerminated(context.Background(), jobID)
		}
		w.mu.Unlock()

		// Close WebSocket connection
		if w.conn != nil {
			w.conn.Close()
		}

		// Stop background services
		if w.resourceMonitor != nil {
			w.resourceMonitor.Stop()
		}

		if w.loadBatcher != nil {
			w.loadBatcher.Stop()
		}

		if w.eventProcessor != nil {
			w.eventProcessor.Stop()
		}

		if w.metricsCollector != nil {
			w.metricsCollector.Stop()
		}

		close(w.doneCh)
	})

	return err
}

// Note: updateJobStatus is already implemented in universal_helpers.go

// UpdateStatus updates the worker's status and load
func (w *UniversalWorker) UpdateStatus(status WorkerStatus, load float32) error {
	w.mu.Lock()
	w.status = status
	jobCount := len(w.activeJobs)
	w.mu.Unlock()

	if w.wsState != WebSocketStateConnected {
		return ErrNotConnected
	}

	statusProto := workerStatusToProto(status)
	msg := &livekit.UpdateWorkerStatus{
		Status:   &statusProto,
		Load:     load,
		JobCount: uint32(jobCount),
	}

	w.logger.Info("[DEBUG] Sending worker status update",
		"status", status,
		"load", load,
		"jobCount", jobCount)

	return w.sendMessage(&livekit.WorkerMessage{
		Message: &livekit.WorkerMessage_UpdateWorker{
			UpdateWorker: msg,
		},
	})
}

// Media capabilities
func (w *UniversalWorker) PublishTrack(jobID string, track webrtc.TrackLocal) (*lksdk.LocalTrackPublication, error) {
	jobCtx, exists := w.getJobContext(jobID)
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	if jobCtx.Room == nil {
		return nil, fmt.Errorf("not connected to room for job %s", jobID)
	}

	return jobCtx.Room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{})
}

// Participant management
func (w *UniversalWorker) GetParticipant(jobID string, identity string) (*lksdk.RemoteParticipant, error) {
	tracker, err := w.getParticipantTracker(jobID)
	if err != nil {
		return nil, err
	}

	return tracker.GetParticipant(identity)
}

func (w *UniversalWorker) GetAllParticipants(jobID string) ([]*lksdk.RemoteParticipant, error) {
	tracker, err := w.getParticipantTracker(jobID)
	if err != nil {
		return nil, err
	}

	return tracker.GetAllParticipants(), nil
}

func (w *UniversalWorker) SendDataToParticipant(jobID string, identity string, data []byte, reliable bool) error {
	jobCtx, exists := w.getJobContext(jobID)
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	if jobCtx.Room == nil {
		return fmt.Errorf("not connected to room for job %s", jobID)
	}

	opts := []lksdk.DataPublishOption{
		lksdk.WithDataPublishDestination([]string{identity}),
	}
	if reliable {
		opts = append(opts, lksdk.WithDataPublishReliable(true))
	}

	return jobCtx.Room.LocalParticipant.PublishData(data, opts...)
}

// Job context access
func (w *UniversalWorker) GetJobContext(jobID string) (*JobContext, bool) {
	return w.getJobContext(jobID)
}

// ===================== Missing Worker Methods =====================

// IsConnected returns whether the worker is connected to the server
func (w *UniversalWorker) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.wsState == WebSocketStateConnected
}

// StopWithTimeout gracefully shuts down the worker with a custom timeout
func (w *UniversalWorker) StopWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	w.stopOnce.Do(func() {
		close(w.stopCh)

		// Execute shutdown hooks
		if w.shutdownHandler != nil {
			w.shutdownHandler.ExecutePhase(ctx, ShutdownPhasePreStop)
		}

		// Clean up resources
		w.mu.Lock()
		if w.conn != nil {
			w.conn.Close()
		}
		w.mu.Unlock()

		// Stop background services
		if w.resourceMonitor != nil {
			w.resourceMonitor.Stop()
		}

		if w.loadBatcher != nil {
			w.loadBatcher.Stop()
		}

		if w.eventProcessor != nil {
			w.eventProcessor.Stop()
		}

		if w.metricsCollector != nil {
			w.metricsCollector.Stop()
		}

		if w.shutdownHandler != nil {
			w.shutdownHandler.ExecutePhase(ctx, ShutdownPhaseCleanup)
		}
	})

	select {
	case <-w.doneCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown timeout after %v", timeout)
	}
}

// QueueJob adds a job to the priority queue when at capacity
func (w *UniversalWorker) QueueJob(job *livekit.Job, token, url string) error {
	if w.jobQueue == nil {
		return fmt.Errorf("job queuing not enabled")
	}

	// Default to normal priority if no calculator is configured
	priority := JobPriorityNormal
	if w.opts.JobPriorityCalculator != nil {
		priority = w.opts.JobPriorityCalculator.CalculatePriority(job)
	}
	return w.jobQueue.Enqueue(job, priority, token, url)
}

// GetJobCheckpoint returns checkpoint for a specific job
func (w *UniversalWorker) GetJobCheckpoint(jobID string) *JobCheckpoint {
	w.jobCheckpointsMu.RLock()
	defer w.jobCheckpointsMu.RUnlock()
	return w.jobCheckpoints[jobID]
}

// Health returns comprehensive health status report
func (w *UniversalWorker) Health() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	health := map[string]interface{}{
		"connected":    w.wsState == WebSocketStateConnected,
		"worker_id":    w.workerID,
		"status":       w.status.String(),
		"active_jobs":  len(w.activeJobs),
		"max_jobs":     w.opts.MaxJobs,
		"healthy":      w.healthCheck.isHealthy,
		"last_ping":    w.healthCheck.lastPing,
		"last_pong":    w.healthCheck.lastPong,
		"missed_pings": w.healthCheck.missedPings,
	}

	if w.jobQueue != nil {
		health["queue_size"] = w.jobQueue.Size()
	}

	if w.resourcePool != nil {
		health["resource_pool"] = map[string]interface{}{
			"size": w.resourcePool.Size(),
		}
	}

	return health
}

// GetMetrics returns operational metrics
func (w *UniversalWorker) GetMetrics() map[string]int64 {
	return map[string]int64{
		"jobs_accepted":  w.metrics.jobsAccepted,
		"jobs_rejected":  w.metrics.jobsRejected,
		"jobs_completed": w.metrics.jobsCompleted,
		"jobs_failed":    w.metrics.jobsFailed,
	}
}

// GetQueueStats returns job queue statistics
func (w *UniversalWorker) GetQueueStats() map[string]interface{} {
	if w.jobQueue == nil {
		return map[string]interface{}{"enabled": false}
	}

	return map[string]interface{}{
		"enabled": true,
		"size":    w.jobQueue.Size(),
	}
}

// GetResourcePoolStats returns resource pool statistics
func (w *UniversalWorker) GetResourcePoolStats() map[string]interface{} {
	if w.resourcePool == nil {
		return map[string]interface{}{"enabled": false}
	}

	// ResourcePool doesn't have GetStats, return basic info
	return map[string]interface{}{
		"enabled": true,
		"size":    w.resourcePool.Size(),
	}
}

// AddShutdownHook adds custom cleanup logic during shutdown
func (w *UniversalWorker) AddShutdownHook(phase ShutdownPhase, hook ShutdownHook) error {
	if w.shutdownHandler == nil {
		return fmt.Errorf("shutdown handler not initialized")
	}
	return w.shutdownHandler.AddHook(phase, hook)
}

// RemoveShutdownHook removes a shutdown hook
func (w *UniversalWorker) RemoveShutdownHook(phase ShutdownPhase, name string) bool {
	if w.shutdownHandler == nil {
		return false
	}
	return w.shutdownHandler.RemoveHook(phase, name)
}

// GetShutdownHooks returns all hooks for a phase
func (w *UniversalWorker) GetShutdownHooks(phase ShutdownPhase) []ShutdownHook {
	if w.shutdownHandler == nil {
		return nil
	}
	return w.shutdownHandler.GetHooks(phase)
}

// AddPreStopHook convenience method for pre-stop hooks
func (w *UniversalWorker) AddPreStopHook(name string, handler func(context.Context) error) error {
	return w.AddShutdownHook(ShutdownPhasePreStop, ShutdownHook{
		Name:    name,
		Handler: handler,
		Timeout: 5 * time.Second,
	})
}

// AddCleanupHook convenience method for cleanup hooks
func (w *UniversalWorker) AddCleanupHook(name string, handler func(context.Context) error) error {
	return w.AddShutdownHook(ShutdownPhaseCleanup, ShutdownHook{
		Name:    name,
		Handler: handler,
		Timeout: 5 * time.Second,
	})
}

// ===================== WorkerInterface Implementation =====================

// GetServerURL returns the server URL
func (w *UniversalWorker) GetServerURL() string {
	return w.serverURL
}

// GetLogger returns the logger instance
func (w *UniversalWorker) GetLogger() Logger {
	return w.logger
}

// GetActiveJobs returns active jobs (for recovery)
func (w *UniversalWorker) GetActiveJobs() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	jobs := make(map[string]interface{})
	for id, job := range w.activeJobs {
		jobs[id] = job
	}
	return jobs
}

// SetActiveJob sets an active job (for recovery)
func (w *UniversalWorker) SetActiveJob(jobID string, job interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Convert activeJob to JobContext for UniversalWorker
	if aj, ok := job.(*activeJob); ok {
		w.activeJobs[jobID] = &JobContext{
			Job:       aj.job,
			Room:      aj.room,
			StartedAt: aj.startedAt,
		}
	} else if jc, ok := job.(*JobContext); ok {
		w.activeJobs[jobID] = jc
	}
}

// ===================== Missing ParticipantAgent Methods =====================

// GetTargetParticipant returns the target participant being monitored
func (w *UniversalWorker) GetTargetParticipant() *lksdk.RemoteParticipant {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.targetParticipant
}

// GetParticipantInfo returns information about a specific participant
func (w *UniversalWorker) GetParticipantInfo(identity string) (*ParticipantInfo, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info, exists := w.participants[identity]
	return info, exists
}

// GetAllParticipantInfo returns information about all participants
func (w *UniversalWorker) GetAllParticipantInfo() map[string]*ParticipantInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[string]*ParticipantInfo)
	for k, v := range w.participants {
		result[k] = v
	}
	return result
}

// RequestPermissionChange requests permission changes for a participant
func (w *UniversalWorker) RequestPermissionChange(jobID string, identity string, permissions *livekit.ParticipantPermission) error {
	jobCtx, exists := w.getJobContext(jobID)
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	if jobCtx.Room == nil {
		return fmt.Errorf("not connected to room for job %s", jobID)
	}

	// This would typically be done via server API
	// For now, return unimplemented
	return fmt.Errorf("permission change not implemented in SDK")
}

// GetPermissionManager returns the permission manager
func (w *UniversalWorker) GetPermissionManager() *ParticipantPermissionManager {
	return w.permissionManager
}

// GetCoordinationManager returns the coordination manager
func (w *UniversalWorker) GetCoordinationManager() *MultiParticipantCoordinator {
	return w.coordinationManager
}

// GetEventProcessor returns the event processor
func (w *UniversalWorker) GetEventProcessor() *UniversalEventProcessor {
	return w.eventProcessor
}

// ===================== Missing PublisherAgent Methods =====================

// SubscribeToTrack manually subscribes to a specific track
func (w *UniversalWorker) SubscribeToTrack(publication *lksdk.RemoteTrackPublication) error {
	if publication == nil {
		return fmt.Errorf("publication is nil")
	}

	// Subscribe to the track
	if err := publication.SetSubscribed(true); err != nil {
		return err
	}

	// Track the subscription
	w.mu.Lock()
	w.subscribedTracks[publication.SID()] = &PublisherTrackSubscription{
		Publication:      publication,
		SubscribedAt:     time.Now(),
		CurrentQuality:   livekit.VideoQuality_HIGH,
		PreferredQuality: livekit.VideoQuality_HIGH,
	}
	w.mu.Unlock()

	return nil
}

// UnsubscribeFromTrack manually unsubscribes from a track
func (w *UniversalWorker) UnsubscribeFromTrack(publication *lksdk.RemoteTrackPublication) error {
	if publication == nil {
		return fmt.Errorf("publication is nil")
	}

	// Unsubscribe from the track
	if err := publication.SetSubscribed(false); err != nil {
		return err
	}

	// Remove from tracking
	w.mu.Lock()
	delete(w.subscribedTracks, publication.SID())
	w.mu.Unlock()

	return nil
}

// EnableTrack enables/disables a track without unsubscribing
func (w *UniversalWorker) EnableTrack(trackSID string, enabled bool) error {
	w.mu.RLock()
	subscription, exists := w.subscribedTracks[trackSID]
	w.mu.RUnlock()

	if !exists {
		return fmt.Errorf("track %s not subscribed", trackSID)
	}

	subscription.Publication.SetEnabled(enabled)
	return nil
}

// SetTrackQuality sets preferred quality for video tracks
func (w *UniversalWorker) SetTrackQuality(trackSID string, quality livekit.VideoQuality) error {
	w.mu.Lock()
	subscription, exists := w.subscribedTracks[trackSID]
	if exists {
		subscription.PreferredQuality = quality
		subscription.LastQualityChange = time.Now()
	}
	w.mu.Unlock()

	if !exists {
		return fmt.Errorf("track %s not subscribed", trackSID)
	}

	// Apply quality change via SDK
	if subscription.Publication != nil {
		return subscription.Publication.SetVideoQuality(quality)
	}
	return nil
}

// SetTrackDimensions sets preferred video dimensions
func (w *UniversalWorker) SetTrackDimensions(trackSID string, width, height uint32) error {
	w.mu.Lock()
	subscription, exists := w.subscribedTracks[trackSID]
	if exists {
		subscription.Dimensions = &VideoDimensions{
			Width:  width,
			Height: height,
		}
	}
	w.mu.Unlock()

	if !exists {
		return fmt.Errorf("track %s not subscribed", trackSID)
	}

	// SDK v2 may not support direct dimension control
	// This would be a simulcast layer selection
	return nil
}

// SetTrackFrameRate sets preferred frame rate for video tracks
func (w *UniversalWorker) SetTrackFrameRate(trackSID string, fps float64) error {
	w.mu.Lock()
	subscription, exists := w.subscribedTracks[trackSID]
	if exists {
		subscription.FrameRate = float32(fps)
	}
	w.mu.Unlock()

	if !exists {
		return fmt.Errorf("track %s not subscribed", trackSID)
	}

	// SDK v2 may not support direct frame rate control
	return nil
}

// GetPublisherInfo returns the currently tracked publisher participant
func (w *UniversalWorker) GetPublisherInfo() *lksdk.RemoteParticipant {
	// In UniversalWorker, any participant can be a publisher
	// Return the first participant with published tracks
	w.mu.RLock()
	defer w.mu.RUnlock()

	for _, jobCtx := range w.activeJobs {
		if jobCtx.Room != nil {
			participants := jobCtx.Room.GetRemoteParticipants()
			for _, p := range participants {
				if len(p.TrackPublications()) > 0 {
					return p
				}
			}
		}
	}

	return nil
}

// GetSubscribedTracks returns all currently subscribed tracks
func (w *UniversalWorker) GetSubscribedTracks() map[string]*PublisherTrackSubscription {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[string]*PublisherTrackSubscription)
	for k, v := range w.subscribedTracks {
		result[k] = v
	}
	return result
}

// Internal methods
func (w *UniversalWorker) getJobContext(jobID string) (*JobContext, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ctx, exists := w.activeJobs[jobID]
	return ctx, exists
}

func (w *UniversalWorker) getParticipantTracker(jobID string) (*ParticipantTracker, error) {
	jobCtx, exists := w.getJobContext(jobID)
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	if jobCtx.Room == nil {
		return nil, fmt.Errorf("not connected to room for job %s", jobID)
	}

	w.mu.RLock()
	tracker, exists := w.participantTrackers[jobCtx.Room.Name()]
	w.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no participant tracker for room %s", jobCtx.Room.Name())
	}

	return tracker, nil
}

// handleJobAssignment processes a job assignment from the server
func (w *UniversalWorker) handleJobAssignment(assignment *livekit.JobAssignment) {
	job := assignment.Job
	if job == nil {
		w.logger.Error("Received job assignment with nil job")
		return
	}

	w.logger.Info("Job assigned",
		"jobID", job.Id,
		"type", job.Type,
		"room", job.Room.Name,
	)

	// Get metadata from handler
	metadata := w.handler.GetJobMetadata(job)
	if metadata == nil {
		metadata = &JobMetadata{}
	}

	// Update metrics
	atomic.AddInt64(&w.metrics.jobsAccepted, 1)

	// Accept the job
	accept := &JobAcceptInfo{
		Identity:       metadata.ParticipantIdentity,
		Name:           metadata.ParticipantName,
		Metadata:       metadata.ParticipantMetadata,
		Attributes:     metadata.ParticipantAttributes,
		SupportsResume: metadata.SupportsResume,
	}

	if err := w.sendJobAccept(job.Id, accept); err != nil {
		w.logger.Error("Failed to accept job", "error", err, "jobID", job.Id)
		w.updateJobStatus(job.Id, livekit.JobStatus_JS_FAILED, err.Error())
		atomic.AddInt64(&w.metrics.jobsFailed, 1)
		return
	}

	// Connect to room with proper callbacks
	roomURL := w.serverURL
	if assignment.Url != nil && *assignment.Url != "" {
		roomURL = *assignment.Url
	}

	// Set up room callbacks
	roomCallback := w.createRoomCallbacks(job)

	// Connect to room
	room, err := lksdk.ConnectToRoomWithToken(roomURL, assignment.Token, roomCallback, lksdk.WithAutoSubscribe(false))
	if err != nil {
		w.logger.Error("Failed to connect to room", "error", err, "jobID", job.Id)
		w.updateJobStatus(job.Id, livekit.JobStatus_JS_FAILED, err.Error())
		return
	}

	// Create job context
	ctx, cancel := context.WithCancel(context.Background())
	jobCtx := &JobContext{
		Job:        job,
		Room:       room,
		Cancel:     cancel,
		StartedAt:  time.Now(),
		CustomData: make(map[string]interface{}),
	}

	// Store job context
	w.mu.Lock()
	w.activeJobs[job.Id] = jobCtx
	w.jobStartTimes[job.Id] = jobCtx.StartedAt
	w.rooms[room.Name()] = room
	w.participantTrackers[room.Name()] = NewParticipantTracker(room)
	w.mu.Unlock()

	// Update job status to running
	w.updateJobStatus(job.Id, livekit.JobStatus_JS_RUNNING, "")

	// Update load
	w.updateLoad()

	// Run job handler
	go w.runJobHandler(ctx, jobCtx)
}

// createRoomCallbacks creates room callbacks that dispatch to the handler
func (w *UniversalWorker) createRoomCallbacks(job *livekit.Job) *lksdk.RoomCallback {
	return &lksdk.RoomCallback{
		OnDisconnected: func() {
			w.logger.Info("Disconnected from room", "jobID", job.Id)
			w.mu.Lock()
			room, exists := w.rooms[job.Room.Name]
			w.mu.Unlock()
			if exists {
				w.handler.OnRoomDisconnected(context.Background(), room, "normal disconnect")
			}
		},
		OnDisconnectedWithReason: func(reason lksdk.DisconnectionReason) {
			w.logger.Info("Disconnected from room with reason", "jobID", job.Id, "reason", reason)
			w.mu.Lock()
			room, exists := w.rooms[job.Room.Name]
			w.mu.Unlock()
			if exists {
				w.handler.OnRoomDisconnected(context.Background(), room, string(reason))
			}

			// Handle specific disconnection reasons
			switch reason {
			case lksdk.DuplicateIdentity:
				w.updateJobStatus(job.Id, livekit.JobStatus_JS_FAILED, "duplicate participant identity")
			case lksdk.ParticipantRemoved:
				w.updateJobStatus(job.Id, livekit.JobStatus_JS_FAILED, "participant was removed from room")
			case lksdk.RoomClosed:
				w.updateJobStatus(job.Id, livekit.JobStatus_JS_FAILED, "room was closed")
			}
		},
		OnReconnecting: func() {
			w.logger.Info("Reconnecting to room", "jobID", job.Id)
		},
		OnReconnected: func() {
			w.logger.Info("Reconnected to room", "jobID", job.Id)
		},
		OnParticipantConnected: func(participant *lksdk.RemoteParticipant) {
			// Track participant info
			w.mu.Lock()
			w.participants[participant.Identity()] = &ParticipantInfo{
				Participant:  participant,
				JoinedAt:     time.Now(),
				LastActivity: time.Now(),
				Permissions:  participant.Permissions(),
			}
			// For PARTICIPANT jobs, track target participant
			if job.Type == livekit.JobType_JT_PARTICIPANT && job.Participant != nil {
				if participant.Identity() == job.Participant.Identity {
					w.targetParticipant = participant
				}
			}
			w.mu.Unlock()

			w.handler.OnParticipantJoined(context.Background(), participant)
		},
		OnParticipantDisconnected: func(participant *lksdk.RemoteParticipant) {
			// Remove participant info
			w.mu.Lock()
			delete(w.participants, participant.Identity())
			if w.targetParticipant != nil && w.targetParticipant.Identity() == participant.Identity() {
				w.targetParticipant = nil
			}
			w.mu.Unlock()

			w.handler.OnParticipantLeft(context.Background(), participant)
		},
		OnActiveSpeakersChanged: func(speakers []lksdk.Participant) {
			w.handler.OnActiveSpeakersChanged(context.Background(), speakers)
		},
		OnRoomMetadataChanged: func(metadata string) {
			w.mu.Lock()
			room, exists := w.rooms[job.Room.Name]
			w.mu.Unlock()
			if exists {
				oldMetadata := room.Metadata()
				w.handler.OnRoomMetadataChanged(context.Background(), oldMetadata, metadata)
			}
		},
		ParticipantCallback: lksdk.ParticipantCallback{
			OnMetadataChanged: func(oldMetadata string, p lksdk.Participant) {
				if remoteParticipant, ok := p.(*lksdk.RemoteParticipant); ok {
					// Update participant info
					w.mu.Lock()
					if info, exists := w.participants[remoteParticipant.Identity()]; exists {
						info.LastActivity = time.Now()
					}
					w.mu.Unlock()

					w.handler.OnParticipantMetadataChanged(context.Background(), remoteParticipant, oldMetadata)
				}
			},
			OnIsSpeakingChanged: func(p lksdk.Participant) {
				if remoteParticipant, ok := p.(*lksdk.RemoteParticipant); ok {
					w.handler.OnParticipantSpeakingChanged(context.Background(), remoteParticipant, p.IsSpeaking())
				}
			},
			OnConnectionQualityChanged: func(update *livekit.ConnectionQualityInfo, p lksdk.Participant) {
				if remoteParticipant, ok := p.(*lksdk.RemoteParticipant); ok {
					w.handler.OnConnectionQualityChanged(context.Background(), remoteParticipant, update.Quality)
				}
			},
			OnTrackMuted: func(pub lksdk.TrackPublication, p lksdk.Participant) {
				w.handler.OnTrackMuted(context.Background(), pub, p)
			},
			OnTrackUnmuted: func(pub lksdk.TrackPublication, p lksdk.Participant) {
				w.handler.OnTrackUnmuted(context.Background(), pub, p)
			},
			OnTrackPublished: func(publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				w.handler.OnTrackPublished(context.Background(), rp, publication)
			},
			OnTrackUnpublished: func(publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				w.handler.OnTrackUnpublished(context.Background(), rp, publication)
			},
			OnTrackSubscribed: func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				// Track subscription for PublisherAgent functionality
				w.mu.Lock()
				w.subscribedTracks[publication.SID()] = &PublisherTrackSubscription{
					Track:            track,
					Publication:      publication,
					SubscribedAt:     time.Now(),
					CurrentQuality:   livekit.VideoQuality_HIGH,
					PreferredQuality: livekit.VideoQuality_HIGH,
				}
				w.mu.Unlock()

				w.handler.OnTrackSubscribed(context.Background(), track, publication, rp)
			},
			OnTrackUnsubscribed: func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				// Remove track subscription
				w.mu.Lock()
				delete(w.subscribedTracks, publication.SID())
				w.mu.Unlock()

				w.handler.OnTrackUnsubscribed(context.Background(), track, publication, rp)
			},
			OnDataPacket: func(data lksdk.DataPacket, params lksdk.DataReceiveParams) {
				if params.Sender != nil {
					protoData := data.ToProto()
					if protoData != nil {
						w.handler.OnDataReceived(context.Background(), protoData.GetUser().GetPayload(), params.Sender, protoData.Kind)
					}
				}
			},
		},
	}
}

// runJobHandler runs the job handler in a goroutine with panic recovery
func (w *UniversalWorker) runJobHandler(ctx context.Context, jobCtx *JobContext) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("Handler panic in OnJobAssigned", "panic", r, "jobID", jobCtx.Job.Id)
			w.updateJobStatus(jobCtx.Job.Id, livekit.JobStatus_JS_FAILED, fmt.Sprintf("handler panic: %v", r))
		}

		// Clean up
		w.mu.Lock()
		delete(w.activeJobs, jobCtx.Job.Id)
		delete(w.jobStartTimes, jobCtx.Job.Id)
		if jobCtx.Room != nil {
			delete(w.rooms, jobCtx.Room.Name())
			delete(w.participantTrackers, jobCtx.Room.Name())
			jobCtx.Room.Disconnect()
		}
		w.mu.Unlock()

		// Update job status
		w.updateJobStatus(jobCtx.Job.Id, livekit.JobStatus_JS_SUCCESS, "")

		// Update metrics
		atomic.AddInt64(&w.metrics.jobsCompleted, 1)

		// Update load
		w.updateLoad()
	}()

	// Handle job-specific setup
	if jobCtx.Job.Type == livekit.JobType_JT_PARTICIPANT && jobCtx.Job.Participant != nil {
		// Find target participant
		for _, p := range jobCtx.Room.GetRemoteParticipants() {
			if p.Identity() == jobCtx.Job.Participant.Identity {
				jobCtx.TargetParticipant = p
				break
			}
		}
	}

	// Notify handler of room connection
	w.handler.OnRoomConnected(ctx, jobCtx.Room)

	// Run job handler
	if err := w.handler.OnJobAssigned(ctx, jobCtx); err != nil {
		w.logger.Error("Job handler error", "error", err, "jobID", jobCtx.Job.Id)
		w.updateJobStatus(jobCtx.Job.Id, livekit.JobStatus_JS_FAILED, err.Error())
	}
}

// from the original Worker class but adapted for the universal architecture
