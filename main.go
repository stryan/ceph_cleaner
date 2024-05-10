package main

import (
	"log"
	"os"
	"slices"
	"strconv"

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
	height := os.Getenv("CEPH_MAX_HEIGHT")
	noclean := os.Getenv("CEPH_NOCLEAN")
	nograph := os.Getenv("CEPH_NOGRAPH")
	clean := true
	graph := true
	if noclean != "" {
		clean = false
	}
	if nograph != "" {
		graph = false
	}

	var maxheight int
	if height != "" {
		var err error
		maxheight, err = strconv.Atoi(height)
		if err != nil {
			log.Fatal(err)
		}
	}
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

	cleanupGraph(conn, pool, true, maxheight, clean, graph)
}

// Stub function to represent checking if something's deleted outside of the backend
func logicalLookupDeleted(r Resource) bool {
	deleted := []string{
		"img2",
		"img4",
	}

	return slices.Contains(deleted, r.Name)
}
