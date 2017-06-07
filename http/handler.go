package http

import (
	"net/http"
	"io"
	"github.com/sosozhuang/paxos/comm"
	"github.com/gorilla/mux"
	"encoding/json"
	"strconv"
	"io/ioutil"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/storage"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "Paxos RESTful Service")
}

func newLeaderHandler(node comm.Node, router *mux.Router) {
	lh := &leaderHandler{node}
	router.HandleFunc("/leader", lh.getLeader).Methods("GET")
}

type LeaderResp struct {
	Leader comm.Member `json:"leader"`
	IsLeader bool `json:"is_leader"`
}

type leaderHandler struct {
	comm.Node
}

func (h *leaderHandler) getLeader(w http.ResponseWriter, r *http.Request) {
	var resp LeaderResp
	resp.Leader = h.Node.GetLeader()
	resp.IsLeader = h.Node.IsLeader()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Errorf("Encode leader response error: %v.", err)
	}
}

func newMemberHandler(m comm.Node, router *mux.Router) {
	mh := &memberHandler{m}
	router.HandleFunc("/members", mh.getMembers).Methods("GET")
	router.HandleFunc("/members", mh.addMember).Methods("POST")
	router.HandleFunc("/members/{id}", mh.getMember).Methods("GET")
	router.HandleFunc("/members/{id}", mh.updateMember).Methods("PUT")
	router.HandleFunc("/members/{id}", mh.removeMember).Methods("DELETE")
}

type memberHandler struct {
	node comm.Node
	//membership comm.Membership
}

func (h *memberHandler) getMembers(w http.ResponseWriter, r *http.Request) {
	members, err := h.node.GetMembers()
	if err != nil {
		log.Errorf("Get members error: %v.", err)
		e := HttpError{err.Error(), http.StatusServiceUnavailable}
		e.WriteTo(w)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(members); err != nil {
		log.Errorf("Encode members error: %v.", err)
	}
}

func (h *memberHandler) getMember(w http.ResponseWriter, r *http.Request) {
	id, ok := mux.Vars(r)["id"]
	if id == "" || !ok {
		e := HttpError{"empty param id", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	var (
		nodeID uint64
		err    error
	)
	if id == "self" {
		nodeID = h.node.GetNodeID()
	} else {
		nodeID, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			e := HttpError{err.Error(), http.StatusBadRequest}
			e.WriteTo(w)
			return
		}
	}
	members, err := h.node.GetMembers()
	if err != nil {
		log.Errorf("Get members error: %v.", err)
		e := HttpError{err.Error(), http.StatusServiceUnavailable}
		e.WriteTo(w)
		return
	}

	member, ok := members[nodeID]
	if !ok {
		e := HttpError{"member not found", http.StatusNotFound}
		e.WriteTo(w)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(member); err != nil {
		log.Errorf("Encode member error: %v.", err)
	}
}

func (h *memberHandler) addMember(w http.ResponseWriter, r *http.Request) {
	member := comm.Member{}
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		log.Errorf("Add member read request body error: %v.", err)
		e := HttpError{err.Error(), http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	if err = r.Body.Close(); err != nil {
		log.Warningf("Add member close request body error: %v.", err)
	}
	if err = json.Unmarshal(body, &member); err != nil {
		log.Errorf("Add member unmarshal request body error: %v.", err)
		e := HttpError{err.Error(), http.StatusBadRequest}
		e.WriteTo(w)
		return
	}

	if err = h.node.AddMember(member); err != nil {
		var code int
		if err == comm.ErrMemberExists {
			code = http.StatusConflict
		} else if err == comm.ErrClusterUninitialized {
			code = http.StatusServiceUnavailable
		} else if err == comm.ErrMemberNameEmpty ||
			err == comm.ErrMemberAddrEmpty ||
			err == comm.ErrMemberServiceUrlEmpty {
			code = http.StatusBadRequest
		} else {
			log.Errorf("Add member error: %v.", err)
			code = http.StatusInternalServerError
		}
		e := HttpError{err.Error(), code}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *memberHandler) updateMember(w http.ResponseWriter, r *http.Request) {
	id, ok := mux.Vars(r)["id"]
	if !ok {
		e := HttpError{"empty param id", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	var (
		nodeID uint64
		err    error
	)
	if id == "self" {
		nodeID = h.node.GetNodeID()
	} else {
		nodeID, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			e := HttpError{err.Error(), http.StatusBadRequest}
			e.WriteTo(w)
			return
		}
	}
	src := comm.Member{
		NodeID: proto.Uint64(nodeID),
	}
	dst := comm.Member{}
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		log.Errorf("Update member read request body error: %v.", err)
		e := HttpError{err.Error(), http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	if err = r.Body.Close(); err != nil {
		log.Warningf("Update member close request body error: %v.", err)
	}
	if err = json.Unmarshal(body, &dst); err != nil {
		log.Errorf("Update member unmarshal request body error: %v.", err)
		e := HttpError{err.Error(), http.StatusBadRequest}
		e.WriteTo(w)
		return
	}

	if err = h.node.UpdateMember(dst, src); err != nil {
		log.Errorf("Update member error: %v.", err)
		var code int
		if err == comm.ErrMemberExists {
			code = http.StatusConflict
		} else if err == comm.ErrMemberNotExists {
			code = http.StatusNotFound
		} else if err == comm.ErrClusterUninitialized {
			code = http.StatusServiceUnavailable
		} else if err == comm.ErrMemberNotModified {
			code = http.StatusNotModified
		} else if err == comm.ErrMemberNameEmpty ||
			err == comm.ErrMemberAddrEmpty ||
			err == comm.ErrMemberServiceUrlEmpty {
			code = http.StatusBadRequest
		} else {
			code = http.StatusInternalServerError
		}
		e := HttpError{err.Error(), code}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *memberHandler) removeMember(w http.ResponseWriter, r *http.Request) {
	id, ok := mux.Vars(r)["id"]
	if !ok {
		e := HttpError{"empty param id", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	var (
		nodeID uint64
		err    error
	)
	if id == "self" {
		nodeID = h.node.GetNodeID()
	} else {
		nodeID, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			e := HttpError{err.Error(), http.StatusBadRequest}
			e.WriteTo(w)
			return
		}
	}
	member := comm.Member{
		NodeID: proto.Uint64(nodeID),
	}
	if err = h.node.RemoveMember(member); err != nil {
		log.Errorf("Remove member error: %v.", err)
		var code int
		if err == comm.ErrMemberNotExists {
			code = http.StatusNotFound
		} else if err == comm.ErrClusterUninitialized {
			code = http.StatusServiceUnavailable
		} else {
			code = http.StatusInternalServerError
		}
		e := HttpError{err.Error(), code}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type keysHandler struct {
	store storage.KeyValueStore
}

func newKeysHandler(store storage.KeyValueStore, router *mux.Router) {
	kh := &keysHandler{store}
	r := router.PathPrefix("/keys/").Subrouter()
	r.HandleFunc("/{path:.*}", kh.get).Methods("GET")
	r.HandleFunc("/{path:.*}", kh.set).Methods("POST")
	r.HandleFunc("/{path:.*}", kh.delete).Methods("DELETE")
}

func (h *keysHandler) set(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys"):]
	if key == "" {
		e := HttpError{"empty key", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	err := r.ParseForm()
	if err != nil {
		e := HttpError{err.Error(), http.StatusBadRequest}
		e.WriteTo(w)
		return
	}
	value := r.FormValue("value")
	if err := h.store.Set(key, value); err != nil {
		e := HttpError{err.Error(), http.StatusServiceUnavailable}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *keysHandler) get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys"):]
	if key == "" {
		e := HttpError{"empty key", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}

	value, err := h.store.Get(key)
	if err != nil {
		var code int
		if err == storage.ErrNotFound {
			code = http.StatusNotFound
		} else {
			code = http.StatusServiceUnavailable
		}
		e := HttpError{err.Error(), code}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, value)
}

func (h *keysHandler) delete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys"):]
	if key == "" {
		e := HttpError{"empty key", http.StatusBadRequest}
		e.WriteTo(w)
		return
	}

	if err := h.store.Delete(key); err != nil {
		e := HttpError{err.Error(), http.StatusServiceUnavailable}
		e.WriteTo(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}