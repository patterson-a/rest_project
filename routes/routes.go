package routes

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"
	"hash/fnv"
	"math"
	"strconv"
	"sync"
)

const locations_set = "rest_project:locations"

type Location string

// So Location is a graph.Node
func (self Location) ID() int64 {
	hasher := fnv.New64()
	hasher.Write([]byte(self))
	return int64(hasher.Sum64())
}

type RouteStore struct {
	sync.Mutex

	graph *simple.WeightedDirectedGraph
	redis redis.Conn
}

type Route struct {
	Route  []string `json:"route"`
	Weight float64  `json:"weight"`
}

func New(conn redis.Conn) *RouteStore {
	var ret RouteStore
	ret.graph = simple.NewWeightedDirectedGraph(0.0, math.Inf(1))
	ret.redis = conn
	return &ret
}

func Restore(conn redis.Conn) (*RouteStore, error) {
	ret := New(conn)
	locations, err := redis.Strings(conn.Do("SMEMBERS", locations_set))
	if err != nil {
		return ret, err
	}

	routes := make(map[string]map[string]float64)
	for _, loc := range locations {
		ret.AddLocation(loc, map[string]float64(nil))
		routes[loc], err = getEdges(conn, loc)
		if err != nil {
			return nil, err
		}
	}

	for from, connected := range routes {
		if ret.AddRoutes(from, connected) != nil {
			return nil, err
		}
	}

	return ret, nil
}

func getEdges(conn redis.Conn, loc string) (map[string]float64, error) {
	stringMap, err := redis.StringMap(conn.Do("HGETALL", loc))
	if err != nil {
		return nil, err
	}

	ret := make(map[string]float64)
	for k, v := range stringMap {
		ret[k], err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// POST /maps/ (with JSON name: string, routes_to: map[string]weight optional) : CREATE a location, optionally with routes
func (rs *RouteStore) AddLocation(name string, routes map[string]float64) error {
	rs.Lock()
	defer rs.Unlock()

	loc := Location(name)
	if rs.graph.Node(loc.ID()) != nil {
		return fmt.Errorf("%s already exists", loc)
	}

	rs.graph.AddNode(loc)
	if _, err := rs.redis.Do("SADD", locations_set, name); err != nil {
		return err
	}

	for to, weight := range routes {
		if name != to {
			rs.graph.SetWeightedEdge(rs.graph.NewWeightedEdge(loc, Location(to), weight))
			if _, err := rs.redis.Do("HSET", name, to, weight); err != nil {
				return err
			}
		}
	}
	return nil
}

// GET  /maps/ : READ a list of all known locations
func (rs *RouteStore) GetLocations() []string {
	rs.Lock()
	defer rs.Unlock()

	nodes := rs.graph.Nodes()
	var ret []string

	for nodes.Next() {
		node := nodes.Node()
		if loc, ok := node.(Location); ok {
			ret = append(ret, string(loc))
		} else {
			ret = append(ret, strconv.FormatInt(node.ID(), 10))
		}
	}

	return ret
}

// GET  /maps/<location> : READ list of places <location> has direct connections to
func (rs *RouteStore) RoutesFrom(name string) ([]string, error) {
	loc := Location(name)
	var ret []string

	rs.Lock()
	defer rs.Unlock()

	if rs.graph.Node(loc.ID()) == nil {
		return ret, fmt.Errorf("%s does not exist", loc)
	}

	nodes := rs.graph.From(loc.ID())

	for nodes.Next() {
		node := nodes.Node()
		if loc, ok := node.(Location); ok {
			ret = append(ret, string(loc))
		} else {
			ret = append(ret, strconv.FormatInt(node.ID(), 10))
		}
	}

	return ret, nil
}

// GET  /maps/<from>/<to> : READ list of shortest routes from <from> to <to>
func (rs *RouteStore) RoutesBetween(fromStr, toStr string) ([]Route, error) {
	rs.Lock()
	defer rs.Unlock()

	from, to := Location(fromStr), Location(toStr)
	var ret []Route

	if rs.graph.Node(from.ID()) == nil {
		return ret, fmt.Errorf("%s does not exist", from)
	}
	if rs.graph.Node(to.ID()) == nil {
		return ret, fmt.Errorf("%s does not exist", to)
	}

	paths, weight := path.DijkstraAllFrom(from, rs.graph).AllTo(to.ID())
	for _, path := range paths {
		route := Route{Weight: weight}
		for _, node := range path {
			if loc, ok := node.(Location); ok {
				route.Route = append(route.Route, string(loc))
			} else {
				route.Route = append(route.Route, strconv.FormatInt(node.ID(), 10))
			}
		}
		ret = append(ret, route)
	}

	return ret, nil
}

// PUT  /maps/add/<location> (with JSON routes_to: map[string]weight) : UPDATE add the given connections to <location>
func (rs *RouteStore) AddRoutes(name string, routes map[string]float64) error {
	rs.Lock()
	defer rs.Unlock()

	loc := Location(name)

	if rs.graph.Node(loc.ID()) == nil {
		return fmt.Errorf("%s does not exist", loc)
	}

	for to, weight := range routes {
		if name != to {
			rs.graph.SetWeightedEdge(rs.graph.NewWeightedEdge(loc, Location(to), weight))
			if _, err := rs.redis.Do("HSET", name, to, weight); err != nil {
				return err
			}
		}
	}
	return nil
}

// PUT  /maps/delete/<location> (with JSON from: []string) : UPDATE remove the given connections from <location>
func (rs *RouteStore) RemoveRoutes(name string, routes []string) error {
	rs.Lock()
	defer rs.Unlock()

	loc := Location(name)

	if rs.graph.Node(loc.ID()) == nil {
		return fmt.Errorf("%s does not exist", loc)
	}

	for _, to := range routes {
		if name != to {
			if _, err := rs.redis.Do("HDEL", name, to); err != nil {
				return err
			}
			rs.graph.RemoveEdge(loc.ID(), Location(to).ID())
		}
	}
	return nil
}

// DELETE /maps/<location> : DELETE the given location (and all edges from/to it) (and error if no such location)
func (rs *RouteStore) DeleteLocation(name string) error {
	rs.Lock()
	defer rs.Unlock()

	loc := Location(name)

	if rs.graph.Node(loc.ID()) == nil {
		return fmt.Errorf("%s does not exist", loc)
	}

	if _, err := rs.redis.Do("SREM", locations_set, name); err != nil {
		return err
	}
	for _, loc := range rs.GetLocations() {
		if _, err := rs.redis.Do("HDEL", loc, name); err != nil {
			return err
		}
	}

	rs.graph.RemoveNode(loc.ID())

	return nil
}
