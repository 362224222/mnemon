package graph

import (
	"github.com/mnemon-dev/mnemon/internal/memory/model"
	"github.com/mnemon-dev/mnemon/internal/memory/store"
)

// BFSNode represents a node discovered during BFS traversal.
type BFSNode struct {
	Insight *model.Insight
	Hop     int
	ViaEdge *model.Edge
}

// BFSOptions controls BFS traversal behavior.
type BFSOptions struct {
	MaxDepth   int            // maximum hop distance from start
	MaxNodes   int            // maximum nodes to return (0 = unlimited)
	EdgeFilter model.EdgeType // filter by edge type (empty = all types)
}

// BFS performs breadth-first traversal from startID over the full graph.
// Pre-loads all active insights and edges to avoid N+1 queries.
// The start node is excluded from results. Only active (non-deleted) nodes are visited.
func BFS(db *store.DB, startID string, opts BFSOptions) []BFSNode {
	allInsights, err := db.GetAllActiveInsights()
	if err != nil {
		return nil
	}
	insightMap := make(map[string]*model.Insight, len(allInsights))
	for _, ins := range allInsights {
		insightMap[ins.ID] = ins
	}

	allEdges, err := db.GetAllEdges()
	if err != nil {
		return nil
	}
	edgeAdj := make(map[string][]*model.Edge)
	for _, e := range allEdges {
		edgeAdj[e.SourceID] = append(edgeAdj[e.SourceID], e)
		if e.SourceID != e.TargetID {
			edgeAdj[e.TargetID] = append(edgeAdj[e.TargetID], e)
		}
	}

	type entry struct {
		id  string
		hop int
	}

	visited := map[string]bool{startID: true}
	queue := []entry{{id: startID, hop: 0}}
	var result []BFSNode

	for len(queue) > 0 {
		if opts.MaxNodes > 0 && len(result) >= opts.MaxNodes {
			break
		}

		cur := queue[0]
		queue = queue[1:]

		if cur.hop >= opts.MaxDepth {
			continue
		}

		for _, edge := range edgeAdj[cur.id] {
			if opts.EdgeFilter != "" && edge.EdgeType != opts.EdgeFilter {
				continue
			}

			neighborID := edge.TargetID
			if neighborID == cur.id {
				neighborID = edge.SourceID
			}

			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			insight := insightMap[neighborID]
			if insight == nil {
				continue // soft-deleted or missing
			}

			result = append(result, BFSNode{
				Insight: insight,
				Hop:     cur.hop + 1,
				ViaEdge: edge,
			})

			if opts.MaxNodes > 0 && len(result) >= opts.MaxNodes {
				break
			}

			queue = append(queue, entry{
				id:  neighborID,
				hop: cur.hop + 1,
			})
		}
	}

	return result
}
