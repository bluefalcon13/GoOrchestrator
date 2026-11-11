package engine

import (
	"fmt"
	"sort"
	"strings"

	"go-orchestrator/llm"
	"go-orchestrator/tasks"
)

// ValidatePlanSpec runs Go-side structural validation on a PlanSpec:
// 1. Verifies non-empty and unique task IDs across the entire plan.
// 2. Verifies that every task_type has a registered TaskHandler.
// 3. Verifies that all DependsOn references point to existing task IDs.
// 4. Disallows self-dependencies.
// 5. Performs cycle detection and topological sorting via Kahn's algorithm.
func ValidatePlanSpec(spec llm.PlanSpec, reg *tasks.Registry) ([]string, error) {
	taskMap := make(map[string]llm.TaskSpec)
	for _, epic := range spec.Epics {
		for _, story := range epic.Stories {
			for _, task := range story.Tasks {
				if strings.TrimSpace(task.ID) == "" {
					return nil, fmt.Errorf("task in story %q has empty ID", story.ID)
				}
				if _, exists := taskMap[task.ID]; exists {
					return nil, fmt.Errorf("duplicate task ID detected: %q", task.ID)
				}
				if reg != nil {
					if _, ok := reg.Get(task.TaskType); !ok {
						return nil, fmt.Errorf("task %q has unregistered task_type %q (registered: %v)",
							task.ID, task.TaskType, reg.List())
					}
				}
				taskMap[task.ID] = task
			}
		}
	}

	if len(taskMap) == 0 {
		return nil, fmt.Errorf("plan %q contains no tasks", spec.Title)
	}

	// Verify dependencies exist and are not self-referential
	for id, task := range taskMap {
		for _, dep := range task.DependsOn {
			if dep == id {
				return nil, fmt.Errorf("task %q cannot depend on itself", id)
			}
			if _, exists := taskMap[dep]; !exists {
				return nil, fmt.Errorf("task %q depends on non-existent task %q", id, dep)
			}
		}
	}

	// Kahn's algorithm for topological sorting and cycle detection
	inDegree := make(map[string]int, len(taskMap))
	adjList := make(map[string][]string, len(taskMap)) // upstream -> list of downstream tasks
	for id := range taskMap {
		inDegree[id] = 0
	}
	for id, task := range taskMap {
		for _, dep := range task.DependsOn {
			inDegree[id]++
			adjList[dep] = append(adjList[dep], id)
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	// Sort queue initially for deterministic ordering
	sort.Strings(queue)

	var sorted []string
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		sorted = append(sorted, curr)

		for _, downstream := range adjList[curr] {
			inDegree[downstream]--
			if inDegree[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
		sort.Strings(queue)
	}

	if len(sorted) < len(taskMap) {
		var cyclicNodes []string
		for id, deg := range inDegree {
			if deg > 0 {
				cyclicNodes = append(cyclicNodes, id)
			}
		}
		sort.Strings(cyclicNodes)
		return nil, fmt.Errorf("cycle detected in DAG involving tasks: %s", strings.Join(cyclicNodes, ", "))
	}

	return sorted, nil
}

// BuildAdjacencyLists constructs upstream-to-downstream and downstream-to-upstream mappings for a plan.
func BuildAdjacencyLists(plan *PlanNode) (downstreamMap map[string][]string, upstreamMap map[string][]string) {
	downstreamMap = make(map[string][]string)
	upstreamMap = make(map[string][]string)

	for _, epic := range plan.Epics {
		for _, story := range epic.Stories {
			for _, task := range story.Tasks {
				upstreamMap[task.ID] = append([]string(nil), task.DependsOn...)
				for _, dep := range task.DependsOn {
					downstreamMap[dep] = append(downstreamMap[dep], task.ID)
				}
			}
		}
	}
	return downstreamMap, upstreamMap
}

// FindTransitiveDependents returns all task IDs that transitively depend on rootTaskID.
func FindTransitiveDependents(rootTaskID string, downstreamMap map[string][]string) []string {
	visited := make(map[string]bool)
	var queue []string

	for _, d := range downstreamMap[rootTaskID] {
		queue = append(queue, d)
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if visited[curr] {
			continue
		}
		visited[curr] = true
		for _, next := range downstreamMap[curr] {
			if !visited[next] {
				queue = append(queue, next)
			}
		}
	}

	result := make([]string, 0, len(visited))
	for id := range visited {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

