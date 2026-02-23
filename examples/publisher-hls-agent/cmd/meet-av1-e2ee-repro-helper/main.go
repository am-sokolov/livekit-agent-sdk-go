package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func main() {
	var (
		livekitHTTPURL      string
		livekitWSURL        string
		apiKey              string
		apiSecret           string
		meetBaseURL         string
		roomName            string
		agentName           string
		participantIdentity string
		participantName     string
		codec               string
		passphrase          string
		dispatchMetadata    string
	)

	flag.StringVar(&livekitHTTPURL, "livekit-http-url", "http://127.0.0.1:7880", "LiveKit HTTP URL used by RoomService")
	flag.StringVar(&livekitWSURL, "livekit-ws-url", "ws://127.0.0.1:7880", "LiveKit WS URL used by the Meet custom URL")
	flag.StringVar(&apiKey, "api-key", "devkey", "LiveKit API key")
	flag.StringVar(&apiSecret, "api-secret", "secret", "LiveKit API secret")
	flag.StringVar(&meetBaseURL, "meet-base-url", "http://127.0.0.1:3000", "Base URL for local meet app")
	flag.StringVar(&roomName, "room", "", "Room name to create (required)")
	flag.StringVar(&agentName, "agent-name", "", "Agent dispatch name to attach to room (required)")
	flag.StringVar(&participantIdentity, "participant-identity", "", "Participant identity used for generated token (required)")
	flag.StringVar(&participantName, "participant-name", "meet-av1-e2ee", "Participant display name used for generated token")
	flag.StringVar(&codec, "codec", "av1", "Meet video codec query parameter")
	flag.StringVar(&passphrase, "passphrase", "test-e2ee-secret-123", "E2EE passphrase hash fragment for Meet URL")
	flag.StringVar(&dispatchMetadata, "dispatch-metadata", `{"record_audio":true,"record_video":true}`, "JSON metadata for room agent dispatch")
	flag.Parse()

	missing := false
	for _, item := range []struct {
		name  string
		value string
	}{
		{"room", roomName},
		{"agent-name", agentName},
		{"participant-identity", participantIdentity},
	} {
		if strings.TrimSpace(item.value) == "" {
			log.Printf("missing required flag: -%s", item.name)
			missing = true
		}
	}
	if missing {
		flag.Usage()
		return
	}

	roomClient := lksdk.NewRoomServiceClient(livekitHTTPURL, apiKey, apiSecret)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _ = roomClient.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: roomName})

	_, err := roomClient.CreateRoom(ctx, &livekit.CreateRoomRequest{
		Name: roomName,
		Agents: []*livekit.RoomAgentDispatch{
			{
				AgentName: agentName,
				Metadata:  dispatchMetadata,
			},
		},
	})
	if err != nil {
		log.Fatalf("create room %q failed: %v", roomName, err)
	}

	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     roomName,
	}
	grant.SetCanPublish(true)
	grant.SetCanSubscribe(true)
	grant.SetCanPublishData(true)

	at := auth.NewAccessToken(apiKey, apiSecret)
	at.SetIdentity(participantIdentity).
		SetName(participantName).
		AddGrant(grant)

	token, err := at.ToJWT()
	if err != nil {
		log.Fatalf("failed to mint participant token: %v", err)
	}

	query := url.Values{}
	query.Set("liveKitUrl", livekitWSURL)
	query.Set("token", token)
	query.Set("codec", codec)

	meetURL := strings.TrimRight(meetBaseURL, "/") + "/custom/?" + query.Encode() + "#" + url.QueryEscape(passphrase)

	fmt.Printf("ROOM_NAME=%s\n", roomName)
	fmt.Printf("AGENT_NAME=%s\n", agentName)
	fmt.Printf("PARTICIPANT_IDENTITY=%s\n", participantIdentity)
	fmt.Printf("MEET_URL=%s\n", meetURL)
}
