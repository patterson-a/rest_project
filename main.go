package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/patterson-a/rest_project/routes"
	"github.com/gomodule/redigo/redis"
	"log"
	"mime"
	"net/http"
	"os"
)

type routeServer struct {
	store *routes.RouteStore
}

func NewRouteServer(conn redis.Conn) *routeServer {
	store, err := routes.Restore(conn)
	if err != nil {
		panic(err)
	}
	return &routeServer{store: store}
}

//// API:
// POST /maps/ (with JSON name: string, routes_to: map[string]weight optional) : CREATE a location, optionally with routes
// GET  /maps/ : READ a list of all known locations
// GET  /maps/<location> : READ list of places <location> has direct connections to
// GET  /maps/<from>/<to> : READ list of shortest routes from <from> to <to>
// PUT  /maps/add/<location> (with JSON to: map[string]weight) : UPDATE add the given connections to <location>
// PUT  /maps/delete/<location> (with JSON from: []string) : UPDATE remove the given connections from <location>
// DELETE /maps/<location> : DELETE the given location (and all edges from/to it) (and error if no such location)

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379",
		redis.DialPassword("bad-password"))
	if err != nil {
		panic(err)
	}

	router := mux.NewRouter()
	router.StrictSlash(true)
	server := NewRouteServer(conn)

	router.HandleFunc("/maps/", server.addLocationHandler).Methods("POST")
	router.HandleFunc("/maps/", server.getLocationsHandler).Methods("GET")
	router.HandleFunc("/maps/{location}/", server.routesFromHandler).Methods("GET")
	router.HandleFunc("/maps/{from}/{to}/", server.routesBetweenHandler).Methods("GET")
	router.HandleFunc("/maps/add/{location}/", server.addRoutesHandler).Methods("PUT")
	router.HandleFunc("/maps/delete/{location}/", server.removeRoutesHandler).Methods("PUT")
	router.HandleFunc("/maps/{location}/", server.deleteLocationHandler).Methods("DELETE")

	var port string
	if envVar := os.Getenv("SERVERPORT"); envVar != "" {
		port = envVar
	} else {
		port = "1337"
	}

	log.Printf("Starting the server on port %s\n", port)
	log.Fatal(http.ListenAndServe("localhost:"+port, router))
}

// POST /maps/ (with JSON name: string, routes_to: map[string]weight optional) : CREATE a location, optionally with routes
func (rs *routeServer) addLocationHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Creating a location from %s\n", req.URL.Path)

	type locationRequest struct {
		Name     string             `json:"name"`
		RoutesTo map[string]float64 `json:"routes_to"`
	}

	mediatype, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mediatype != "application/json" {
		http.Error(w, "requires application/json Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	var lr locationRequest
	if err := dec.Decode(&lr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := rs.store.AddLocation(lr.Name, lr.RoutesTo); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func renderJSON(w http.ResponseWriter, v interface{}) {
	js, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("JSON Marshalling failure: %s", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// GET  /maps/ : READ a list of all known locations
func (rs *routeServer) getLocationsHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Getting locations at %s\n", req.URL.Path)

	locations := rs.store.GetLocations()
	renderJSON(w, locations)
}

// GET  /maps/<location> : READ list of places <location> has direct connections to
func (rs *routeServer) routesFromHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Getting locations from a location at %s\n", req.URL.Path)

	loc := mux.Vars(req)["location"]

	locations, err := rs.store.RoutesFrom(loc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	renderJSON(w, locations)
}

// GET  /maps/<from>/<to> : READ list of shortest routes from <from> to <to>
func (rs *routeServer) routesBetweenHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Finding routes at %s\n", req.URL.Path)

	vars := mux.Vars(req)
	from, to := vars["from"], vars["to"]

	routes, err := rs.store.RoutesBetween(from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	renderJSON(w, routes)
}

// PUT  /maps/add/<location> (with JSON to: map[string]weight) : UPDATE add the given connections to <location>
func (rs *routeServer) addRoutesHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Adding routes at %s\n", req.URL.Path)

	loc := mux.Vars(req)["location"]

	mediatype, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mediatype != "application/json" {
		http.Error(w, "requires application/json Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	dec := json.NewDecoder(req.Body)
	var routes map[string]float64
	if err := dec.Decode(&routes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if rs.store.AddRoutes(loc, routes) != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// PUT  /maps/delete/<location> (with JSON from: []string) : UPDATE remove the given connections from <location>
func (rs *routeServer) removeRoutesHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Deleting routes at %s\n", req.URL.Path)

	loc := mux.Vars(req)["location"]

	mediatype, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mediatype != "application/json" {
		http.Error(w, "requires application/json Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	dec := json.NewDecoder(req.Body)
	var routes []string
	if err := dec.Decode(&routes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if rs.store.RemoveRoutes(loc, routes) != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// DELETE /maps/<location> : DELETE the given location (and all edges from/to it) (and error if no such location)
func (rs *routeServer) deleteLocationHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Deleting location at %s\n", req.URL.Path)

	loc := mux.Vars(req)["location"]

	if err := rs.store.DeleteLocation(loc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
