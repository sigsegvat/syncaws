package main

import (
	"github.com/pborman/uuid"
	"io/ioutil"
	"encoding/json"
	"crypto/rand"
)

type config struct {
	Bucket string
	Key [] byte
}

func (c * config) New() {
	c.Bucket = uuid.New()
	c.Key = make([]byte,32)
	rand.Read(c.Key)
}

func (c *config) Read(p string) error {

	if data, err := ioutil.ReadFile(p); err == nil {
		if err := json.Unmarshal(data, c); err == nil {
			return nil
		}else {
			return err
		}
	}else {
		return err
	}

}

func (c * config) Write(p string) error {
	if data, e := json.Marshal(c); e == nil {
		if e := ioutil.WriteFile(p, data, 0700); e == nil {
			return nil
		}    else {
			return e
		}
	}else {
		return e
	}


}

func (c *config) ReadOrNew(p string) error {
	e := c.Read(p+"/.s3p")
	if e != nil {
		c.New()
	}

	if e := c.Write(p+"/.s3p"); e!=nil {
		return e
	}

	return nil
}