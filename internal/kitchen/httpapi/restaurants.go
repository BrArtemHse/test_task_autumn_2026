package httpapi

import (
	"errors"
	"net/http"
	"strings"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
)

func (h *Handler) listRestaurants(w http.ResponseWriter, r *http.Request) {
	restaurants, err := h.catalog.ListRestaurants(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "catalog_error", "catalog is unavailable")
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Restaurants []restaurantResponse `json:"restaurants"`
	}{
		Restaurants: mapRestaurants(restaurants),
	})
}

func (h *Handler) getRestaurantMenu(w http.ResponseWriter, r *http.Request) {
	restaurantID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/restaurants/"), "/menu")
	if restaurantID == "" {
		writeError(w, http.StatusNotFound, "not_found", "restaurant not found")
		return
	}

	menu, err := h.catalog.GetMenu(r.Context(), restaurantID)
	if err != nil {
		if errors.Is(err, catalogdomain.ErrRestaurantNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "restaurant not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "catalog_error", "catalog is unavailable")
		return
	}

	writeJSON(w, http.StatusOK, mapMenu(menu))
}
