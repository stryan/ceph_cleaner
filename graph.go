package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

func TimeStamp() string {
	ts := time.Now().UTC().Format(time.RFC3339)
	return strings.Replace(strings.Replace(ts, ":", "", -1), "-", "", -1)
}

func cleanupGraph(conn *rados.Conn, pool string, dry bool, maxHeight int, clean bool, makegraph bool) {
	prefix := fmt.Sprintf("graph-%v", TimeStamp())
	ioc, err := conn.OpenIOContext(pool)
	if err != nil {
		log.Fatalf("error opening pool context %v", err)
	}
	resourceHash := func(r Resource) string {
		return r.Name
	}
	roots := []Resource{}
	forest := graph.New(resourceHash, graph.Directed(), graph.Acyclic())
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
			Name: v,
			Type: rImage,
		}
		node.Alive = !logicalLookupDeleted(node)
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
			snapname := fmt.Sprintf("%v/%v", img.GetName(), s.Name)
			snapRes := Resource{
				Name: snapname,
				Type: rSnap,
			}
			snapRes.Alive = !logicalLookupDeleted(snapRes)
			if !snapRes.Alive {
				err = forest.AddVertex(snapRes, graph.VertexAttribute("colorscheme", "reds3"), graph.VertexAttribute("style", "filled"), graph.VertexAttribute("color", "2"), graph.VertexAttribute("fillcolor", "1"))
				if err != nil {
					log.Fatalf("error adding snapshot vertex %v", err)
				}
			} else {
				err = forest.AddVertex(snapRes, graph.VertexAttribute("colorscheme", "greens3"), graph.VertexAttribute("style", "filled"), graph.VertexAttribute("color", "2"), graph.VertexAttribute("fillcolor", "1"))
				if err != nil {
					log.Fatalf("error adding snapshot vertex %v", err)
				}
			}
		}
		if !node.Alive {
			err = forest.AddVertex(node, graph.VertexAttribute("colorscheme", "reds3"), graph.VertexAttribute("style", "filled"), graph.VertexAttribute("color", "2"), graph.VertexAttribute("fillcolor", "1"))
			if err != nil {
				log.Fatalf("error adding image vertex %v", err)
			}
		} else {
			err = forest.AddVertex(node, graph.VertexAttribute("colorscheme", "greens3"), graph.VertexAttribute("style", "filled"), graph.VertexAttribute("color", "2"), graph.VertexAttribute("fillcolor", "1"))
			if err != nil {
				log.Fatalf("error adding image vertex %v", err)
			}
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
			snapname := fmt.Sprintf("%v/%v", img.GetName(), s.Name)
			err = forest.AddEdge(v, snapname)
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
			snapname := fmt.Sprintf("%v/%v", pinfo.Image.ImageName, pinfo.Snap.SnapName)

			err = forest.AddEdge(snapname, c)
			if err != nil {
				log.Fatalf("error adding edge from snapshot to child %v", err)
			}
		}
	}
	if makegraph {
		file, _ := os.Create(fmt.Sprintf("graphs/%v-before.gv", prefix))
		defer func() { _ = file.Close() }()

		_ = draw.DOT(forest, file)
	}
	if !clean {
		return
	}
	var cleaned []Resource
	for _, node := range roots {
		log.Printf("Starting cleanup for tree rooted at %v", node.Name)
		iter := 0
		dirty := true
		for dirty && iter < 5 {
			paths, err := forest.AdjacencyMap()
			if err != nil {
				log.Fatal(err)
			}
			backPaths, err := forest.PredecessorMap()
			if err != nil {
				log.Fatal(err)
			}
			iter++
			var deleted []Resource
			var newroots []Resource
			if maxHeight == 0 {
				deleted = trimTree(forest, paths, node)
			} else {
				newroots, deleted = trimTreeWithFlatten(forest, paths, node, maxHeight)
			}

			if len(deleted) == 0 || (len(deleted) == 1 && deleted[0].Name == node.Name) && len(newroots) == 0 {
				dirty = false
			}
			// break incoming edges to flattened resources to create new trees
			for _, v := range newroots {
				for _, p := range backPaths[v.Name] {
					log.Printf("deleting edge %v->%v", p.Source, p.Target)
					err = forest.RemoveEdge(p.Source, p.Target)
					if err != nil {
						log.Fatalf("error deleting edge %v->%v :%v", p.Source, p.Target, err)
					}
				}
				roots = append(roots, v)
			}

			for _, v := range deleted {
				for _, p := range paths[v.Name] {
					log.Printf("deleting edge %v->%v", p.Source, p.Target)
					err = forest.RemoveEdge(p.Source, p.Target)
					if err != nil {
						log.Fatalf("error deleting edge %v->%v :%v", p.Source, p.Target, err)
					}
				}
				for _, p := range backPaths[v.Name] {
					log.Printf("deleting edge %v->%v", p.Source, p.Target)
					err = forest.RemoveEdge(p.Source, p.Target)
					if err != nil {
						log.Fatalf("error deleting edge %v->%v :%v", p.Source, p.Target, err)
					}
				}
				err = forest.RemoveVertex(v.Name)
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
		log.Println("would delete/flatten resources in ceph now")
	}

	log.Printf("Deleted the following resources: %v", cleaned)
	if makegraph {

		afile, _ := os.Create(fmt.Sprintf("graphs/%v-after.gv", prefix))
		defer func() { _ = afile.Close() }()
		_ = draw.DOT(forest, afile)
	}
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

// example version that also flattens if it passes a height limit. I left flattening out for the first version
// since it was supposed to be simple and idealy flattening would be caught at create time.
// untested
func trimTreeWithFlatten(g graph.Graph[string, Resource], p map[string]map[string]graph.Edge[string], node Resource, maxHeight int) ([]Resource, []Resource) {
	deleted := []Resource{}
	newroots := []Resource{}
	s := stack[Resource]{}
	h := stack[int]{}
	s.Push(node)
	h.Push(0)

	for !s.Empty() {
		cur := s.Pop()
		height := h.Pop()
		if height > maxHeight {
			newroots = append(newroots, cur)
			// don't push the children since this is becomming a seperate tree
			continue
		}
		for _, v := range p[cur.Name] {
			child, _ := g.Vertex(v.Target)
			s.Push(child)
			h.Push(height + 1)
		}
		if len(p[cur.Name]) == 0 && logicalLookupDeleted(cur) {
			deleted = append(deleted, cur)
		}
	}
	return newroots, deleted
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
