package http

import (
	"net/http"
	"github.com/sosozhuang/go-paxos/logger"
	"encoding/json"
	"net/url"
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/gorilla/mux"
	"context"
	"time"
	defaultLog "log"
	"io/ioutil"
	"github.com/sosozhuang/go-paxos/storage"
)

var log = logger.GetLogger("http")

type HttpServiceConfig struct {
	ServiceUrl  string
	serviceAddr string
}

func (cfg *HttpServiceConfig) validate() error {
	u, err := url.ParseRequestURI(cfg.ServiceUrl)
	cfg.serviceAddr = u.Host
	return err
}

type HttpService interface {
	Start(chan<- error)
	Stop()
}

type httpService struct {
	cfg    *HttpServiceConfig
	server http.Server
}

func NewHttpService(cfg HttpServiceConfig, node comm.Node, store storage.KeyValueStore) (HttpService, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	service := &httpService{
		cfg:    &cfg,
	}
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", indexHandler)
	newLeaderHandler(node, router)
	newMemberHandler(node, router)
	newKeysHandler(store, router)
	router.HandleFunc("/{path:.*}", http.NotFound)
	service.server = http.Server{
		Addr:        cfg.serviceAddr,
		Handler:     router,
		IdleTimeout: time.Minute * 2,
		MaxHeaderBytes: 1024*100,
		ErrorLog: defaultLog.New(ioutil.Discard, "", defaultLog.Ldate),
	}
	return service, nil
}

func (s *httpService) Start(errChan chan<- error) {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()
}

func (s *httpService) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Errorf("Http service shutdown error: %v.", err)
	}
}

type HttpError struct {
	Message string `json:"message"`
	Code    int    `json:"-"`
}

func (e HttpError) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(e.Code)
	b, err := json.Marshal(e)
	if err != nil {
		log.Errorf("Marshal HttpError error: %v.", err)
		return
	}
	if _, err = w.Write(b); err != nil {
		log.Errorf("Write HttpError response failed: %v.", err)
	}
}