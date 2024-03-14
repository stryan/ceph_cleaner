package main

import (
	"errors"
	"log"
	"os"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

func cleanupGraph(conn *rados.Conn, pool string, dry bool) {
	ioc, err := conn.OpenIOContext(pool)
	if err != nil {
		log.Fatalf("error opening pool context %v", err)
	}
	resourceHash := func(r Resource) string {
		return r.Name
	}
	roots := []Resource{}
	graph := graph.New(resourceHash, graph.Directed(), graph.Acyclic())
	images, err := rbd.GetImageNames(ioc)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("building graph")
	for _, v := range images {
		log.Printf("inspecting vertex %v", v)
		img, err := rbd.OpenImage(ioc, v, rbd.NoSnapshot)
		defer func() { _ = img.Close() }()
		if err != nil {
			log.Fatalf("error opening image %v", err)
		}
		node := Resource{
			Name:  v,
			Type:  rImage,
			Alive: true,
		}
		_, err = img.GetParent()
		if errors.Is(err, rbd.ErrNotFound) {
			roots = append(roots, node)
		} else if err != nil {
			log.Fatalf("error getting parent %v", err)
		}

		snaps, err := img.GetSnapshotNames()
		if err != nil {
			log.Fatalf("error listing snapshots %v", err)
		}
		for _, s := range snaps {
			err = graph.AddVertex(Resource{
				Name:  s.Name,
				Type:  rSnap,
				Alive: false,
			})
			if err != nil {
				log.Fatalf("error adding snapshot vertex %v", err)
			}
		}
		if len(snaps) > 0 {
			node.Alive = true
		}
		err = graph.AddVertex(node)
		if err != nil {
			log.Fatalf("error adding image vertex %v", err)
		}
	}

	for _, v := range images {
		log.Printf("adding edges for %v", v)
		img, err := rbd.OpenImage(ioc, v, rbd.NoSnapshot)
		defer func() { _ = img.Close() }()
		if err != nil {
			log.Fatalf("error opening image %v", err)
		}

		snaps, err := img.GetSnapshotNames()
		if err != nil {
			log.Fatalf("error listing snapshots %v", err)
		}
		for _, s := range snaps {
			err = graph.AddEdge(v, s.Name)
			if err != nil {
				log.Fatalf("error adding image->snap relation %v", err)
			}
		}

		// we're assuming they're all in the same pool here
		_, children, err := img.ListChildren()
		if err != nil {
			log.Fatalf("error listing children %v", err)
		}
		for _, c := range children {
			log.Printf("adding parent-child %v-%v", v, c)
			childImage, err := rbd.OpenImage(ioc, c, rbd.NoSnapshot)
			defer func() { _ = childImage.Close() }()
			if err != nil {
				log.Fatalf("error opening child image %v", err)
			}
			pinfo, err := childImage.GetParent()
			if err != nil {
				log.Fatalf("error getting parent %v", err)
			}
			err = graph.AddEdge(pinfo.Snap.SnapName, c)
			if err != nil {
				log.Fatalf("error adding edge from snapshot to child %v", err)
			}
		}
	}
	file, _ := os.Create("./before.gv")
	defer func() { _ = file.Close() }()

	_ = draw.DOT(graph, file)

	var cleaned []Resource
	for _, node := range roots {
		log.Printf("Starting cleanup for tree rooted at %v", node.Name)
		iter := 0
		dirty := true
		for dirty && iter < 5 {
			paths, err := graph.AdjacencyMap()
			if err != nil {
				log.Fatal(err)
			}
			backPaths, err := graph.PredecessorMap()
			if err != nil {
				log.Fatal(err)
			}
			iter++
			deleted := trimTree(graph, paths, node)
			if len(deleted) == 0 || (len(deleted) == 1 && deleted[0].Name == node.Name) {
				dirty = false
			}
			for _, v := range deleted {
				for _, p := range paths[v.Name] {
					log.Printf("deleting edge %v->%v", p.Source, p.Target)
					err = graph.RemoveEdge(p.Source, p.Target)
					if err != nil {
						log.Fatalf("error deleting edge %v->%v :%v", p.Source, p.Target, err)
					}
				}
				for _, p := range backPaths[v.Name] {
					log.Printf("deleting edge %v->%v", p.Source, p.Target)
					err = graph.RemoveEdge(p.Source, p.Target)
					if err != nil {
						log.Fatalf("error deleting edge %v->%v :%v", p.Source, p.Target, err)
					}
				}
				err = graph.RemoveVertex(v.Name)
				if err != nil {
					log.Fatalf("error deleting vertex %v: %v", v.Name, err)
				}
				cleaned = append(cleaned, v)
			}
			log.Printf("deleted %v", deleted)
		}
		log.Printf("Took %v generations to clean tree rooted at %v", iter, node.Name)
	}
	if !dry {
		log.Println("would deleted resources in ceph now")
	}

	log.Printf("Deleted the following resources: %v", cleaned)
	afile, _ := os.Create("./after.gv")
	defer func() { _ = afile.Close() }()
	_ = draw.DOT(graph, afile)
}

func trimTree(g graph.Graph[string, Resource], p map[string]map[string]graph.Edge[string], node Resource) []Resource {
	deleted := []Resource{}
	s := stack[Resource]{}
	s.Push(node)

	for !s.Empty() {
		cur := s.Pop()
		for _, v := range p[cur.Name] {
			child, _ := g.Vertex(v.Target)
			s.Push(child)
		}
		if len(p[cur.Name]) == 0 && logicalLookupDeleted(cur) {
			deleted = append(deleted, cur)
		}
	}
	return deleted
}

// Generic stack stolen from SO
type stack[V any] []V

func (s *stack[V]) Push(v V) int {
	*s = append(*s, v)
	return len(*s)
}

func (s *stack[V]) Last() V {
	l := len(*s)

	// Upto the developer to handle an empty stack
	if l == 0 {
		log.Fatal("Empty Stack")
	}

	last := (*s)[l-1]
	return last
}

func (s *stack[V]) Empty() bool {
	return len(*s) == 0
}

func (s *stack[V]) Pop() V {
	removed := (*s).Last()
	*s = (*s)[:len(*s)-1]

	return removed
}
