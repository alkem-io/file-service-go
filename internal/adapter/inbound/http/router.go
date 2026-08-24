package http

import (
	"encoding/json"
	"expvar"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// LiveResponse is the body returned by GET /live.
type LiveResponse struct {
	Status string `json:"status"`
}

// ServeLive is the K8s liveness handler: returns 200 unconditionally so the
// probe reflects process-alive only, independent of DB/NATS availability.
func ServeLive(w http.ResponseWriter, _ *http.Request) {
	LiveResponse{Status: "alive"}.Render(w)
}

// Render writes the response as JSON with HTTP 200.
func (r LiveResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// Deps contains all handler dependencies.
type Deps struct {
	PublicHandler   *PublicHandler
	DocumentHandler *DocumentHandler
	HealthHandler   *HealthHandler
	Logger          *zap.Logger
}

// NewRouter creates the chi router with all route groups and middleware.
func NewRouter(deps Deps) *chi.Mux {
	r := chi.NewRouter()

	r.Use(chimw.Recoverer)
	r.Use(RequestID)
	r.Use(RequestLogger(deps.Logger))
	r.Use(responseWriteDeadline)

	// Liveness (K8s livenessProbe): process-alive check only, no dependencies.
	// Kept separate from /health so liveness failures don't restart pods when
	// only a dependency is down.
	r.Get("/live", ServeLive)

	// Readiness (K8s readinessProbe): checks DB (and NATS if configured).
	r.Method("GET", "/health", deps.HealthHandler)

	// Public endpoints. Traefik's `alkemio-resolve` forwardAuth establishes
	// identity at the gateway and stamps `X-Alkemio-Actor-Id`; we trust that
	// header. Strip-prefix in Traefik turns `/api/private/rest/storage/...`
	// into `/rest/storage/...` before it arrives here.
	r.Route("/rest/storage", func(r chi.Router) {
		r.Use(ActorHeaderExtractor)
		r.Get("/document/{id}", deps.PublicHandler.ServeDocument) // backward compat alias
		r.Get("/file/{id}", deps.PublicHandler.ServeDocument)
	})

	// Internal endpoints (no auth)
	r.Route("/internal", func(r chi.Router) {
		r.Post("/file", deps.DocumentHandler.Create)
		r.Post("/file/copy", deps.DocumentHandler.Copy)
		r.Post("/file/content-batch", deps.DocumentHandler.ContentBatch)
		r.Get("/file/{id}/meta", deps.DocumentHandler.GetMeta)
		r.Get("/file/{id}/content", deps.DocumentHandler.GetContent)
		// Content-addressed read by SHA3-256 hash (workspace#008); rationale on GetBlobContent.
		r.Get("/blob/{hash}/content", deps.DocumentHandler.GetBlobContent)
		r.Put("/file/{id}/content", deps.DocumentHandler.ReplaceContent)
		r.Delete("/file/{id}", deps.DocumentHandler.Delete)
		r.Patch("/file/{id}", deps.DocumentHandler.Update)

		// Debug/metrics
		r.Handle("/debug/vars", expvar.Handler())
	})

	return r
}
