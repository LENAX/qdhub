// Package sync contains the sync domain services.
package sync

import (
	"fmt"
	"strings"
)

// DependencyResolverImpl implements DependencyResolver interface.
type DependencyResolverImpl struct{}

// NewDependencyResolver creates a new DependencyResolver.
func NewDependencyResolver() DependencyResolver {
	return &DependencyResolverImpl{}
}

// Resolve 解析依赖关系
// 算法：
// 1. 构建依赖图（API -> []依赖的 API）
// 2. BFS 查找所有依赖（包括传递依赖）
// 3. 检测循环依赖
// 4. 拓扑排序生成执行层级
// 5. 为每个 API 生成 TaskConfig
func (r *DependencyResolverImpl) Resolve(
	selectedAPIs []string,
	allAPIDependencies map[string][]ParamDependency,
) (*ExecutionGraph, []string, error) {
	// 1. 构建依赖图
	depGraph := r.buildDependencyGraph(allAPIDependencies)

	// 2. 查找所有需要的 API（包括传递依赖）
	allAPIs, missingAPIs := r.findAllDependencies(selectedAPIs, depGraph)

	// 3. 检测循环依赖
	if err := r.detectCycle(allAPIs, depGraph); err != nil {
		return nil, nil, err
	}

	// 4. 拓扑排序生成执行层级
	levels, err := r.topologicalSort(allAPIs, depGraph)
	if err != nil {
		return nil, nil, err
	}

	// 5. 为每个 API 生成 TaskConfig
	taskConfigs := r.generateTaskConfigs(allAPIs, allAPIDependencies, depGraph)

	// 构建执行图
	graph := &ExecutionGraph{
		Levels:      levels,
		MissingAPIs: missingAPIs,
		TaskConfigs: taskConfigs,
	}

	// 生成完整的 API 列表（按拓扑顺序）
	var resolvedAPIs []string
	for _, level := range levels {
		resolvedAPIs = append(resolvedAPIs, level...)
	}

	return graph, resolvedAPIs, nil
}

// buildDependencyGraph 构建依赖图
// 返回：API -> []依赖的上游 API
func (r *DependencyResolverImpl) buildDependencyGraph(
	allAPIDependencies map[string][]ParamDependency,
) map[string][]string {
	graph := make(map[string][]string)

	for api, deps := range allAPIDependencies {
		seen := make(map[string]bool)
		for _, dep := range deps {
			if dep.SourceAPI != "" && !seen[dep.SourceAPI] {
				seen[dep.SourceAPI] = true
				graph[api] = append(graph[api], dep.SourceAPI)
			}
		}
	}

	return graph
}

// findAllDependencies 查找所有依赖（包括传递依赖）
// 返回：所有需要的 API、自动补充的 API
func (r *DependencyResolverImpl) findAllDependencies(
	selectedAPIs []string,
	depGraph map[string][]string,
) (allAPIs []string, missingAPIs []string) {
	needed := make(map[string]bool)
	selected := make(map[string]bool)

	// 标记用户选择的 API
	for _, api := range selectedAPIs {
		selected[api] = true
		needed[api] = true
	}

	// BFS 查找所有依赖
	queue := append([]string{}, selectedAPIs...)
	for len(queue) > 0 {
		api := queue[0]
		queue = queue[1:]

		for _, dep := range depGraph[api] {
			if !needed[dep] {
				needed[dep] = true
				queue = append(queue, dep)
			}
		}
	}

	// 分离用户选择的和自动补充的
	for api := range needed {
		allAPIs = append(allAPIs, api)
		if !selected[api] {
			missingAPIs = append(missingAPIs, api)
		}
	}

	return allAPIs, missingAPIs
}

// detectCycle 检测循环依赖
func (r *DependencyResolverImpl) detectCycle(
	apis []string,
	depGraph map[string][]string,
) error {
	// 状态：0=未访问，1=访问中，2=已完成
	state := make(map[string]int)
	var path []string

	var dfs func(api string) error
	dfs = func(api string) error {
		if state[api] == 1 {
			// 找到循环，构建路径
			cycleStart := 0
			for i, a := range path {
				if a == api {
					cycleStart = i
					break
				}
			}
			cyclePath := append(path[cycleStart:], api)
			return fmt.Errorf("circular dependency detected: %s", strings.Join(cyclePath, " -> "))
		}
		if state[api] == 2 {
			return nil
		}

		state[api] = 1
		path = append(path, api)

		for _, dep := range depGraph[api] {
			if err := dfs(dep); err != nil {
				return err
			}
		}

		path = path[:len(path)-1]
		state[api] = 2
		return nil
	}

	for _, api := range apis {
		if state[api] == 0 {
			if err := dfs(api); err != nil {
				return err
			}
		}
	}

	return nil
}

// topologicalSort 拓扑排序生成执行层级
func (r *DependencyResolverImpl) topologicalSort(
	apis []string,
	depGraph map[string][]string,
) ([][]string, error) {
	// 构建入度表（在需要的 API 范围内）
	apiSet := make(map[string]bool)
	for _, api := range apis {
		apiSet[api] = true
	}

	inDegree := make(map[string]int)
	for _, api := range apis {
		inDegree[api] = 0
	}

	// 计算入度（只考虑需要的 API）
	for _, api := range apis {
		for _, dep := range depGraph[api] {
			if apiSet[dep] {
				inDegree[api]++
			}
		}
	}

	// Kahn's algorithm with levels
	var levels [][]string
	remaining := make(map[string]bool)
	for _, api := range apis {
		remaining[api] = true
	}

	for len(remaining) > 0 {
		// 找到当前层的所有节点（入度为0）
		var currentLevel []string
		for api := range remaining {
			if inDegree[api] == 0 {
				currentLevel = append(currentLevel, api)
			}
		}

		if len(currentLevel) == 0 {
			// 不应该发生（已检测循环）
			return nil, fmt.Errorf("topological sort failed: remaining nodes have no zero in-degree")
		}

		// 移除当前层，更新入度
		for _, api := range currentLevel {
			delete(remaining, api)

			// 更新依赖此 API 的其他 API 的入度
			for other := range remaining {
				for _, dep := range depGraph[other] {
					if dep == api {
						inDegree[other]--
					}
				}
			}
		}

		levels = append(levels, currentLevel)
	}

	return levels, nil
}

// generateTaskConfigs 为每个 API 生成 TaskConfig
func (r *DependencyResolverImpl) generateTaskConfigs(
	apis []string,
	allAPIDependencies map[string][]ParamDependency,
	depGraph map[string][]string,
) map[string]*TaskConfig {
	configs := make(map[string]*TaskConfig)

	for _, api := range apis {
		deps := allAPIDependencies[api]

		// Fix B: when an API has dependency edges but no ParamDependency entries,
		// infer dependencies from the upstream API names as a last-resort defense.
		if len(deps) == 0 && len(depGraph[api]) > 0 {
			deps = r.inferDependenciesFromGraph(api, depGraph[api])
			allAPIDependencies[api] = deps
		}

		config := &TaskConfig{
			APIName:      api,
			SyncMode:     r.determineSyncMode(deps),
			Dependencies: r.generateDependencies(depGraph[api]),
		}

		// 生成参数映射
		if len(deps) > 0 {
			config.ParamMappings = r.convertToParamMappings(deps)
		}

		configs[api] = config
	}

	return configs
}

// inferDependenciesFromGraph infers ParamDependency from dependency graph edges when
// explicit ParamDependency configuration is missing. This is a last-resort defense
// to prevent APIs from incorrectly using direct mode.
func (r *DependencyResolverImpl) inferDependenciesFromGraph(api string, sourceAPIs []string) []ParamDependency {
	var deps []ParamDependency
	for _, src := range sourceAPIs {
		switch src {
		case "trade_cal":
			deps = append(deps, ParamDependency{
				ParamName:   "trade_date",
				SourceAPI:   "trade_cal",
				SourceField: "cal_date",
				IsList:      true,
			})
		case "stock_basic":
			deps = append(deps, ParamDependency{
				ParamName:   "ts_code",
				SourceAPI:   "stock_basic",
				SourceField: "ts_code",
				IsList:      true,
			})
		case "index_basic":
			paramName := "ts_code"
			if api == "index_weight" {
				paramName = "index_code"
			}
			deps = append(deps, ParamDependency{
				ParamName:   paramName,
				SourceAPI:   "index_basic",
				SourceField: "ts_code",
				IsList:      true,
			})
		}
	}
	return deps
}

// determineSyncMode 确定同步模式
func (r *DependencyResolverImpl) determineSyncMode(deps []ParamDependency) TaskSyncMode {
	// 如果有 IsList=true 的参数依赖，则使用模板模式
	for _, dep := range deps {
		if dep.IsList {
			return TaskSyncModeTemplate
		}
	}
	return TaskSyncModeDirect
}

// generateDependencies 生成任务依赖列表
func (r *DependencyResolverImpl) generateDependencies(sourceAPIs []string) []string {
	var deps []string
	for _, api := range sourceAPIs {
		// 将 API 名称转换为任务名称
		// 基础 API 使用内置任务名称
		taskName := r.apiToTaskName(api)
		deps = append(deps, taskName)
	}
	return deps
}

// apiToTaskName 将 API 名称转换为任务名称
func (r *DependencyResolverImpl) apiToTaskName(api string) string {
	// 基础 API 使用内置任务名称
	switch api {
	case "trade_cal":
		return "FetchTradeCal"
	case "stock_basic":
		return "FetchStockBasic"
	case "index_basic":
		return "FetchIndexBasic"
	default:
		return "Sync_" + api
	}
}

// convertToParamMappings 转换参数依赖为参数映射
func (r *DependencyResolverImpl) convertToParamMappings(deps []ParamDependency) []ParamMapping {
	var mappings []ParamMapping

	for _, dep := range deps {
		mapping := ParamMapping{
			ParamName:   dep.ParamName,
			SourceTask:  r.apiToTaskName(dep.SourceAPI),
			SourceField: dep.SourceField,
			IsList:      dep.IsList,
			FilterField: dep.FilterField,
			FilterValue: dep.FilterValue,
		}

		// 设置默认选择策略
		if dep.IsList {
			mapping.Select = "all"
		} else {
			mapping.Select = "last"
		}

		mappings = append(mappings, mapping)
	}

	return mappings
}
