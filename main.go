package main

import (
	"log"
	"os"

	"github.com/ceph/go-ceph/rados"
)

type ResourceType int

const (
	rImage ResourceType = iota
	rSnap
)

type Resource struct {
	Name     string
	Alive    bool
	Type     ResourceType
	Children []*Resource
}

func main() {
	conffile := os.Getenv("CEPH_CONF")
	keyfile := os.Getenv("CEPH_KEYRING")
	pool := os.Getenv("CEPH_POOL")
	log.Printf("Using following config locations: %v %v %v", conffile, keyfile, pool)
	conn, _ := rados.NewConn()
	err := conn.ReadConfigFile(conffile)
	if err != nil {
		log.Fatalf("Err reading config: %v", err)
	}
	err = conn.ReadConfigFile(keyfile)
	if err != nil {
		log.Fatalf("Err reading keyring: %v", err)
	}
	err = conn.Connect()
	if err != nil {
		log.Fatalf("error connecting: %v", err)
	}
	defer conn.Shutdown()
	log.Println("connected to ceph")

	cleanupGraph(conn, pool, true)
}

// Stub function to represent checking if something's deleted outside of the backend
func logicalLookupDeleted(r Resource) bool {
	return r.Name != "snap-04303f0d801d544e6"
}
