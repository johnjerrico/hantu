package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/johnjerrico/hantu/redis"
)

type Scheduler interface {
	Register(domain, name string) error
	Active() error
	Sleep() error
	Retire() error
	Shutdown() error
}

func New(host, port string, ttl uint64) Scheduler {
	rr := roundrobin{}
	if host != "" {
		if port == "" {
			port = "6379"
		}
		if ttl == 0 {
			ttl = uint64(1000000000)
		}
		rr.con = redis.New(host, port, ttl)
		rr.distributed = true
	}
	return &rr
}

type roundrobin struct {
	distributed bool
	con         redis.Redis
	domain      string
	id          string
}

func (e *roundrobin) Register(domain, id string) error {
	if !e.distributed {
		return nil
	}
	e.domain = domain
	e.id = id
	request := make(map[string]string)
	request["node"] = e.id
	request["status"] = "candidate"
	marshalled, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled)
}

func (e *roundrobin) Active() error {
	if !e.distributed {
		return nil
	}
	var self map[string]string
	var elected map[string]string
	tmp, err := e.con.HGetData(fmt.Sprintf("%s:nodes", e.domain), e.id)
	if err != nil {
		tostring := string(tmp)
		isnil := strings.Contains(tostring, "redigo: nil")
		if !isnil {
			return err
		} else {
			self = make(map[string]string)
			self["node"] = e.id
			self["status"] = "candidate"
			if marshalled, err := json.Marshal(self); err != nil {
				return err
			} else {
				if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled); err != nil {
					return err
				}
			}

		}
		return err
	} else if len(tmp) > 0 {
		if err := json.Unmarshal(tmp, &self); err != nil {
			return err
		}
	}
	tmp, err = e.con.GetData(fmt.Sprintf("%s:elected_node", e.domain))
	if err != nil {
		isnil := strings.Contains(err.Error(), "redigo: nil")
		if !isnil {
			return err
		}

	} else if len(tmp) > 0 {
		if err := json.Unmarshal(tmp, &elected); err != nil {
			return err
		}
	}
	if elected != nil && self["node"] != elected["node"] {
		return errors.New("node not eligible to govern")
	}
	//election
	if self["status"] == "candidate" {
		self["status"] = "active"
		bytes, err := json.Marshal(self)
		if err != nil {
			return err
		}
		if err := e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), bytes); err != nil {
			return err
		}
		if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, bytes); err != nil {
			return err
		}
	} else if self["status"] == "timeoff" {
		if self["cycle"] == "1" {
			self["cycle"] = "2"
		} else if self["cycle"] == "2" {
			self["status"] = "candidate"
			self["cycle"] = ""
		}
		bytes, err := json.Marshal(self)
		if err != nil {
			return err
		}
		if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, bytes); err != nil {
			return err
		}
	}
	if self["status"] != "active" {
		return errors.New("node not eligible to govern")
	}
	return nil
}

func (e *roundrobin) Sleep() error {
	if !e.distributed {
		return nil
	}
	request := make(map[string]string)
	request["node"] = e.id
	request["status"] = "timeoff"
	request["cycle"] = "1"
	marshalled, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled); err != nil {
		return err
	}
	if err := e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), []byte("")); err != nil {
		return err
	}
	return nil
}

func (e *roundrobin) Retire() error {
	if !e.distributed {
		return nil
	}
	if err := e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), []byte("")); err != nil {
		return err
	}
	if err := e.con.HDelete(fmt.Sprintf("%s:nodes", e.domain), e.id); err != nil {
		return err
	}
	return nil
}

func (e *roundrobin) Shutdown() error {
	if e.con != nil {
		return e.con.Close()
	}
	return nil
}
