package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewServer(t *testing.T) {
	server := NewServer()
	assert.NotNil(t, server)
}

func TestSetupRouter(t *testing.T) {
	server := NewServer()
	router := server.SetupRouter()
	assert.NotNil(t, router)
}

func TestHealthEndpoint(t *testing.T) {
	server := NewServer()
	router := server.SetupRouter()

	req, _ := http.NewRequest("GET", "/_monitoring/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"status":"UP"}`, w.Body.String())
}
