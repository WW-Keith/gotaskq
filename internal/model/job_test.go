package model

import (
	"encoding/json"
	"testing"
)

func TestJob_Valid(t *testing.T) {
	tests := []struct {
		name    string
		job     Job
		wantErr bool
	}{
		{
			name:    "valid job",
			job:     Job{Type: "send email", Payload: json.RawMessage(`{"to":"a@b.com"}`)},
			wantErr: false,
		},
		{
			name:    "missing type",
			job:     Job{Payload: json.RawMessage(`{}`)},
			wantErr: true,
		},
		{
			name:    "missing payload",
			job:     Job{Type: "send_email"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Valid()
			if (err != nil) != tt.wantErr {
				t.Errorf("Valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_IsFinal(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusPending, false},
		{StatusRunning, false},
		{StatusCancelled, true},
		{StatusSucceeded, true},
		{StatusFailed, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			j := Job{Status: tt.status}
			if got := j.IsFinal(); got != tt.want {
				t.Errorf("IsFinal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_JSONRoundTrip(t *testing.T) {
	original := Job{
		ID:      "abc-123",
		Type:    "send_email",
		Payload: json.RawMessage(`{"to":"test@example.com"}`),
		Status:  StatusPending,
		Queue:   "default",
	}

	//Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed %v", err)
	}

	//Unmarshal back
	var decoded Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed %v", err)
	}

	//Check if fields survived the round trip
	if decoded.ID != original.ID {
		t.Errorf("ID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Status != original.Status {
		t.Errorf("Status: got %q, want %q", decoded.Status, original.Status)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Errorf("Payload: got %q, want %q", decoded.Payload, original.Payload)
	}
}
