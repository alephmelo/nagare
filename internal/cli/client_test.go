package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_Headers(t *testing.T) {
	var capturedAuth string
	var capturedAccept string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		capturedAccept = r.Header.Get("Accept")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`)) //nolint:errcheck // test mock
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "supersecret")

	// Test GET sets auth
	_, err := client.Get("/test")
	if err != nil {
		t.Fatalf("unexpected GET error: %v", err)
	}
	if capturedAuth != "Bearer supersecret" {
		t.Errorf("expected Auth header 'Bearer supersecret', got %q", capturedAuth)
	}

	capturedAuth = "" // reset

	// Test POST sets auth
	_, err = client.Post("/test", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected POST error: %v", err)
	}
	if capturedAuth != "Bearer supersecret" {
		t.Errorf("expected Auth header 'Bearer supersecret', got %q", capturedAuth)
	}

	capturedAuth = ""
	capturedAccept = ""

	// Test SSE sets auth and accept header
	err = client.StreamSSE("/sse", func(line string) {})
	if err != nil {
		t.Fatalf("unexpected SSE error: %v", err)
	}
	if capturedAuth != "Bearer supersecret" {
		t.Errorf("expected Auth header 'Bearer supersecret', got %q", capturedAuth)
	}
	if capturedAccept != "text/event-stream" {
		t.Errorf("expected Accept header 'text/event-stream', got %q", capturedAccept)
	}
}

func TestClient_ErrorHandling(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/404" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.URL.Path == "/500" {
			http.Error(w, "internal explosion", http.StatusInternalServerError)
			return
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "")

	tests := []struct {
		name      string
		path      string
		wantError string
	}{
		{"404 Not Found", "/404", "server returned 404: not found"},
		{"500 Internal Error", "/500", "server returned 500: internal explosion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Get(tt.path)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if err.Error() != tt.wantError {
				t.Errorf("expected error %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestClient_PostPayloads(t *testing.T) {
	var capturedBody map[string]interface{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", r.Header.Get("Content-Type"))
		}
		json.NewDecoder(r.Body).Decode(&capturedBody) //nolint:errcheck // test mock
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`)) //nolint:errcheck // test mock
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "")
	_, err := client.Post("/test", map[string]interface{}{"key": "value", "num": 42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedBody["key"] != "value" {
		t.Errorf("expected body to contain key 'value', got %v", capturedBody["key"])
	}
	// json unmarshals numbers into float64 implicitly
	if num, ok := capturedBody["num"].(float64); !ok || num != 42 {
		t.Errorf("expected body to contain num 42, got %v", capturedBody["num"])
	}
}
