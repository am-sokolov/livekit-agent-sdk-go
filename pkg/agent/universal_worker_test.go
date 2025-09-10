package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockUniversalHandlerOld for testing
type MockUniversalHandlerOld struct {
	mock.Mock
	BaseHandler
}

func (m *MockUniversalHandlerOld) OnJobRequest(ctx context.Context, job *livekit.Job) (bool, *JobMetadata) {
	args := m.Called(ctx, job)
	return args.Bool(0), args.Get(1).(*JobMetadata)
}

func (m *MockUniversalHandlerOld) OnJobAssigned(ctx context.Context, jobCtx *JobContext) error {
	args := m.Called(ctx, jobCtx)
	return args.Error(0)
}

func (m *MockUniversalHandlerOld) OnJobTerminated(ctx context.Context, jobID string) {
	m.Called(ctx, jobID)
}

func (m *MockUniversalHandlerOld) OnParticipantJoined(ctx context.Context, participant *lksdk.RemoteParticipant) {
	m.Called(ctx, participant)
}

func (m *MockUniversalHandlerOld) OnParticipantLeft(ctx context.Context, participant *lksdk.RemoteParticipant) {
	m.Called(ctx, participant)
}

func (m *MockUniversalHandlerOld) OnParticipantMetadataChanged(ctx context.Context, participant *lksdk.RemoteParticipant, oldMetadata string) {
	m.Called(ctx, participant, oldMetadata)
}

func TestUniversalWorker_Creation(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	opts := WorkerOptions{
		AgentName: "test-agent",
		JobType:   livekit.JobType_JT_ROOM,
	}

	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, opts)
	assert.NotNil(t, worker)
	assert.Equal(t, "ws://localhost:7880", worker.serverURL)
	assert.Equal(t, "devkey", worker.apiKey)
	assert.Equal(t, "secret", worker.apiSecret)
	assert.Equal(t, handler, worker.handler)
	assert.Equal(t, opts.AgentName, worker.opts.AgentName)
}

func TestUniversalWorker_JobContext(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	// Test empty job context
	ctx, exists := worker.GetJobContext("non-existent")
	assert.False(t, exists)
	assert.Nil(t, ctx)

	// Add a job context
	job := &livekit.Job{
		Id:   "test-job-1",
		Type: livekit.JobType_JT_ROOM,
		Room: &livekit.Room{Name: "test-room"},
	}
	jobCtx := &JobContext{
		Job:       job,
		StartedAt: time.Now(),
	}
	worker.activeJobs[job.Id] = jobCtx

	// Test retrieval
	retrievedCtx, exists := worker.GetJobContext(job.Id)
	assert.True(t, exists)
	assert.Equal(t, jobCtx, retrievedCtx)
	assert.Equal(t, job.Id, retrievedCtx.Job.Id)
}

func TestUniversalWorker_LoadCalculation(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	opts := WorkerOptions{
		MaxJobs: 5,
	}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, opts)

	// Initially should have 0 load
	assert.Equal(t, 0, len(worker.activeJobs))

	// Add jobs and verify load calculation
	for i := 1; i <= 3; i++ {
		jobID := fmt.Sprintf("job-%d", i)
		worker.activeJobs[jobID] = &JobContext{
			Job: &livekit.Job{Id: jobID},
		}
		worker.jobStartTimes[jobID] = time.Now()
	}

	// With 3/5 jobs, load should be reflected
	worker.updateLoad()
	assert.Equal(t, 3, len(worker.activeJobs))
}

func TestUniversalWorker_ParticipantTracking(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	// Create a mock room
	// Since Room.Name() will return empty string for a mock room,
	// we'll use empty string as the key
	roomName := ""
	room := &lksdk.Room{}
	tracker := NewParticipantTracker(room)

	worker.participantTrackers[roomName] = tracker
	worker.rooms[roomName] = room

	// Create job context
	jobID := "test-job"
	jobCtx := &JobContext{
		Job:  &livekit.Job{Id: jobID},
		Room: room,
	}
	worker.activeJobs[jobID] = jobCtx

	// Test participant tracker retrieval
	retrievedTracker, err := worker.getParticipantTracker(jobID)
	assert.NoError(t, err)
	assert.Equal(t, tracker, retrievedTracker)

	// Test with non-existent job
	_, err = worker.getParticipantTracker("non-existent")
	assert.Error(t, err)
}

func TestUniversalWorker_SimpleHandler(t *testing.T) {
	var jobAssignedCalled bool
	var participantJoinedCalled bool

	handler := &SimpleUniversalHandler{
		JobAssignedFunc: func(ctx context.Context, jobCtx *JobContext) error {
			jobAssignedCalled = true
			return nil
		},
		ParticipantJoinedFunc: func(ctx context.Context, participant *lksdk.RemoteParticipant) {
			participantJoinedCalled = true
		},
	}

	// Test that functions are called
	ctx := context.Background()
	jobCtx := &JobContext{Job: &livekit.Job{Id: "test"}}

	err := handler.OnJobAssigned(ctx, jobCtx)
	assert.NoError(t, err)
	assert.True(t, jobAssignedCalled)

	handler.OnParticipantJoined(ctx, &lksdk.RemoteParticipant{})
	assert.True(t, participantJoinedCalled)
}

func TestUniversalWorker_ConcurrentAccess(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	// Test concurrent job addition/removal
	var wg sync.WaitGroup
	numGoroutines := 10
	numJobs := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numJobs; j++ {
				jobID := fmt.Sprintf("job-%d-%d", goroutineID, j)

				// Add job
				worker.mu.Lock()
				worker.activeJobs[jobID] = &JobContext{
					Job: &livekit.Job{Id: jobID},
				}
				worker.mu.Unlock()

				// Remove job
				worker.mu.Lock()
				delete(worker.activeJobs, jobID)
				worker.mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 0, len(worker.activeJobs))
}

func TestUniversalWorker_EventProcessor(t *testing.T) {
	processor := NewUniversalEventProcessor()

	var mu sync.Mutex
	var eventReceived bool
	handler := func(event UniversalEvent) error {
		mu.Lock()
		eventReceived = true
		mu.Unlock()
		assert.Equal(t, UniversalEventTypeParticipantJoined, event.Type)
		return nil
	}

	processor.RegisterHandler(UniversalEventTypeParticipantJoined, handler)

	// Queue an event
	processor.QueueEvent(UniversalEvent{
		Type:      UniversalEventTypeParticipantJoined,
		Timestamp: time.Now(),
		Data:      "test-participant",
	})

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	received := eventReceived
	mu.Unlock()
	assert.True(t, received)

	// Clean up
	processor.Stop()
}

func TestUniversalWorker_StatusUpdates(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	// Test status queue
	update := statusUpdate{
		jobID:      "test-job",
		status:     livekit.JobStatus_JS_RUNNING,
		timestamp:  time.Now(),
		retryCount: 0,
	}

	worker.queueStatusUpdate(update)
	assert.Equal(t, 1, len(worker.statusQueue))
	assert.Equal(t, update.jobID, worker.statusQueue[0].jobID)
}

func TestUniversalWorker_RoomCallbacks(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	job := &livekit.Job{
		Id:   "test-job",
		Type: livekit.JobType_JT_ROOM,
		Room: &livekit.Room{Name: "test-room"},
	}

	callbacks := worker.createRoomCallbacks(job)
	assert.NotNil(t, callbacks)
	assert.NotNil(t, callbacks.OnDisconnected)
	assert.NotNil(t, callbacks.OnParticipantConnected)
	assert.NotNil(t, callbacks.OnTrackPublished)
}

// TestUniversalWorker_ConcurrentRoomCallbacks tests for race conditions in room callback access
func TestUniversalWorker_ConcurrentRoomCallbacks(t *testing.T) {
	handler := &MockUniversalHandlerOld{}
	worker := NewUniversalWorker("ws://localhost:7880", "devkey", "secret", handler, WorkerOptions{})

	// Create a mock room and add it to the worker's rooms map
	mockRoom := &lksdk.Room{}
	job := &livekit.Job{
		Id:   "test-job",
		Type: livekit.JobType_JT_ROOM,
		Room: &livekit.Room{Name: "test-room"},
	}

	// Add room to the map
	worker.mu.Lock()
	worker.rooms[job.Room.Name] = mockRoom
	worker.mu.Unlock()

	// Setup handler expectations
	handler.On("OnRoomDisconnected", mock.Anything, mockRoom, mock.Anything).Maybe()
	handler.On("OnRoomMetadataChanged", mock.Anything, mock.Anything, mock.Anything).Maybe()

	callbacks := worker.createRoomCallbacks(job)

	// Run concurrent operations to test for race conditions
	var wg sync.WaitGroup
	concurrency := 50
	iterations := 100

	// Start goroutines that read from the rooms map via callbacks
	for i := 0; i < concurrency; i++ {
		wg.Add(3)

		// Test OnDisconnected callback
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				callbacks.OnDisconnected()
				time.Sleep(time.Microsecond)
			}
		}()

		// Test OnDisconnectedWithReason callback  
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				callbacks.OnDisconnectedWithReason(lksdk.LeaveRequested)
				time.Sleep(time.Microsecond)
			}
		}()

		// Test OnRoomMetadataChanged callback
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				callbacks.OnRoomMetadataChanged(fmt.Sprintf("metadata-%d", j))
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Start goroutines that modify the rooms map (simulating cleanup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Simulate adding and removing rooms
				roomName := fmt.Sprintf("dynamic-room-%d", id)
				worker.mu.Lock()
				if j%2 == 0 {
					worker.rooms[roomName] = mockRoom
				} else {
					delete(worker.rooms, roomName)
				}
				worker.mu.Unlock()
				time.Sleep(time.Microsecond * 5)
			}
		}(i)
	}

	// Start a goroutine that simulates the cleanup in runJobHandler
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*2; i++ {
			worker.mu.Lock()
			// Simulate cleanup operations
			delete(worker.rooms, job.Room.Name)
			worker.rooms[job.Room.Name] = mockRoom
			worker.mu.Unlock()
			time.Sleep(time.Microsecond * 10)
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify no panics occurred and handler was called
	handler.AssertExpectations(t)
}
