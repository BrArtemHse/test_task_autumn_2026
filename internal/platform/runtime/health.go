package runtime

import (
	"encoding/json"
	"net/http"
)

func NewHealthHandler(serviceName string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(struct {
			Service string `json:"service"`
			Status  string `json:"status"`
		}{
			Service: serviceName,
			Status:  "ok",
		})
	})
}
