# Example Ceph Cleanup Tool
This repo contains an example of a rooted-tree approached to cleaning up ceph clones and snapshots. It can also be used to generate nice graphs of the current parent-child relationships in a Ceph pool.

A rooted image tree is a relation between two or more images caused by cloning i.e if img2 is a clone of img1 and img is a clone of img2, that that is a rooted image tree, rooted at img1. Looks like this: img1 -> img2 -> img3

## What this tool does
- generates nice graphs of all images/snapshots in ceph
- remove "dead" leaf nodes from Ceph image trees
- operates directly on the Ceph layer, though it expects some external source for determining if a Ceph resource is still in use

## What this tool does not do
- Compress Ceph trees to their smallest extent (i.e. if two internal resource nodes are no longer needed, this tool will not flatten the child node of those nodes to create separate, smaller trees)
- actually delete things from ceph

# How to use
Configuration is done through environment variables:
```
CEPH_CONF: locaton of ceph.conf config file
CEPH_KEYING: location of ceph.admin.keyring config file
CEPH_POOL: what pool to iterate over
CEPH_MAX_HEIGHT: (EXPERIMENTAL) Set to a number to have the tool  flatten out trees over this height
CEPH_NOCLEAN: Set to anything to just generate a graph
CEPH_NOGRAPH: Set to anything to skip generating a graph.
```

You'll probably want to edit the `logicalLookupDeleted` function to reflect the external "is resource in use" operation.


Running the tool will result in a `before.gv` and `after.gv` that represent the pool's before and after GC states. You can convert these to PNGs using the `make_pngs.sh` script. This requires `graphviz` to be installed.
