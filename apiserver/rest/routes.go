package rest

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (s *Server) initRouter() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/liveness", s.isAlive).Methods(http.MethodGet)
	return router
}

func (s *Server) isAlive(w http.ResponseWriter, r *http.Request) {
	if !s.IsAlive() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
