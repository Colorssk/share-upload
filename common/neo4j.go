package common

import (
	"context"
	"errors"
	"file-upload/dataModels"
	utils "file-upload/util"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"gopkg.in/ini.v1"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//var (
//	sessionMutex sync.Mutex
//)

// 创建neo4j 连接
func NewNeo4JConn(ctx context.Context, cfg *ini.File) (neo4jDriver neo4j.DriverWithContext, err error) {
	connectUri := fmt.Sprintf("neo4j://%s:%s", cfg.Section("neo4j").Key("uri"), cfg.Section("neo4j").Key("port"))
	neo4jDriver, err = neo4j.NewDriverWithContext(connectUri, neo4j.BasicAuth(cfg.Section("neo4j").Key("user").String(), cfg.Section("neo4j").Key("password").String(), ""))
	if err != nil {
		fmt.Println("连接neo4j失败", err)
		return nil, err
	}
	return neo4jDriver, err
}

// 创建节点-返回属性
func ReturnAttribute(node dataModels.FileNode) string {
	return fmt.Sprintf("isDir: %d,name: \"%s\",  updateTime: \"%s\", type: \"%s\", size: %d, fileHash: \"%s\", fileName: \"%s\", nodeType:\"%s\"", node.IsDir, node.Name, node.UpdateTime, node.Type, node.Size, node.FileHash, node.FileName, node.NodeType)
}

// 批量插入节点，设置关系(文件夹上传),
func InsertNodes(driver neo4j.DriverWithContext, ctx context.Context, cepher string, node dataModels.InputFileInfo, isResotre bool) (isOk bool, err error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	targetNodes := make([]string, 0)
	relativeNodes := make([]string, 0)
	normalizedTargetPath := filepath.Clean(node.TargetPath)
	normalizedRelativePath := filepath.Clean(node.RelativePath)
	if strings.TrimSpace(node.TargetPath) != "" {
		targetNodes = strings.Split(normalizedTargetPath, "\\")
	}
	if strings.TrimSpace(node.RelativePath) != "" {
		relativeNodes = strings.Split(normalizedRelativePath, "\\")
	}
	if len(targetNodes) == 0 {
		return false, errors.New("未指定目标租户")
	}
	// 开始事务
	tx, err := session.BeginTransaction(ctx)
	defer tx.Close(ctx)
	if err != nil {
		fmt.Println("事务开始失败")
		return false, err
	}
	if len(cepher) == 0 {
		// 1： 保证根节点创建
		rootNode, err := GetSingleNodesByRoot(driver, ctx, "", "nodeType", "root")
		if err != nil {
			return false, errors.New("匹配根节点失败")
		}
		if rootNode == nil {
			addRootCepher := fmt.Sprintf("MERGE (root:NodeAttribute{name: \"remoteDisk\",updateTime: \"2023011\",isDir: 1, type: \"directory\",size: 0, fileHash: \"\", fileName:\"remoteDisk\", nodeType: \"root\"})")
			_, err := tx.Run(
				ctx,
				addRootCepher,
				nil)
			if err != nil {
				fmt.Println("neo4j插入根节点失败")
				// 如果提交失败，回滚事务
				if rErr := tx.Rollback(ctx); rErr != nil {
					return false, rErr
				}
				return false, err
			}
		}
		// 2: 确保操作的是自己的用户节点，这个由外部server判断(重要)
		fmt.Println("当前操作的租户:", ctx.Value("name"))
		if ctx.Value("name") != targetNodes[0] && ctx.Value("name") != "admin" {
			return false, errors.New("无权限操作上传操作")
		}
		// 3： 默认传入方法的路径包含: 租户+存储路径 = targetPath+relativePath 均是合法路径
		// 重要， 路径的合法性， 不允许有节点名称叫remoeDish trashDish
		if utils.Contains(targetNodes, "remoeDish") || utils.Contains(relativeNodes, "trashDish") {
			return false, errors.New("存在违规节点名称{\"remoteDish\",\"remoteDish\"]")
		}
		// -> 路径拼接
		totalNodes := make([]string, 0)
		totalNodes = append(totalNodes, targetNodes...)
		totalNodes = append(totalNodes, relativeNodes...)
		totalPathNodes := make([][]string, 0)
		for index, _ := range totalNodes {
			for curIndex := 0; curIndex <= index; curIndex++ {
				if index >= len(totalPathNodes) {
					// 如果索引超出了切片长度，先扩展切片
					for i := len(totalPathNodes); i <= index; i++ {
						totalPathNodes = append(totalPathNodes, []string{})
					}
				}
				totalPathNodes[index] = append(totalPathNodes[index], totalNodes[curIndex])
			}
		}
		faultIndex := -1
		childTree := make([]string, 0) // 断层处开始的子树
		existTree := make([]string, 0) // 断层上面的最短路径
		for index, paths := range totalPathNodes {
			// 根据路径判断节点是否存在， 不存在则新建
			fileNode, err := GetNodeWithPath(driver, ctx, paths)
			if err != nil {
				return false, err
			}
			if fileNode != nil {
				// 已经存在节点，判断下， 除了叶子节点，其余上游节点必须是目录
				if index != len(totalPathNodes)-1 && fileNode.IsDir != 1 {
					return false, errors.New("目标存储路径的上游存在异常节点(非目录)!")
				}
				continue
			} else {
				// 找到断层节点
				faultIndex = index
				childTree = append(childTree, totalPathNodes[len(totalPathNodes)-1][index:]...)
				existTree = append(existTree, totalPathNodes[len(totalPathNodes)-1][0:index]...)
				break
			}
		}
		// 在断层处，拿到子树， 然后入节点（无需指定租户， 只需要指定改在目录就行，所以接收的路径长度>=1）
		if len(childTree) != 0 {
			cepherInsert := ""
			if faultIndex == 0 {
				// 边界情况： 不存在租户, 指定租户挂载在根节点上 **新建+挂载租户**
				cepherInsert += fmt.Sprintf("MATCH (root: NodeAttribute {name: \"remoteDisk\"})\n")
			} else {
				// 查找断层处,末端节点
				insertPoint, err := GetNodeWithPath(driver, ctx, existTree)
				if err != nil {
					return false, err
				}
				// 断层处节点需要是目录， 才被允许继续添加节点
				if insertPoint.IsDir != 1 {
					return false, errors.New("非文件夹下，无法添加文件!")
				}
				cepherInsert += fmt.Sprintf("MATCH (root: NodeAttribute {fileName: \"%s\"})\n", insertPoint.FileName)
			}
			cepherConnections := ""
			for index, nodeName := range childTree {
				// 生成新节点并且，设置关系
				uniqueFileName, err := utils.GenerateUniqueString(nodeName+time.Now().Format("2006-01-02 15:04:05"), 50)
				if err != nil {
					return false, err
				}
				nodeTypeTemp := "normal"
				// 断层的地方是租户并且当前插入的节点是租户
				if faultIndex == 0 && index == 0 {
					nodeTypeTemp = "user"
				} else {
					nodeTypeTemp = "normal"
				}
				var newNodeInfo *dataModels.FileNode
				// 不是叶子节点，到这一步就是合法的默认目录
				if index != len(childTree)-1 {
					newNodeInfo = &dataModels.FileNode{
						IsDir:      1,
						Name:       nodeName,
						UpdateTime: time.Now().Format("2006-01-02 15:04:05"),
						Type:       "directory",
						Size:       0,
						FileHash:   "",
						FileName:   uniqueFileName,
						NodeType:   nodeTypeTemp,
					}
				} else {
					fileNameTemp := uniqueFileName
					// 对于添加的最后一个节点 假如是文件， 为了保持和文件系统中文件名称一致: 必填+fileName一致+唯一性 (前提: 需要是文件)
					if node.IsDir != 1 {
						// 文件 走判断， 因为文件需要存储在文件系统中，所以这块fileName从外部获取
						if len(node.FileName) == 0 {
							return false, errors.New("请指定文件fileName")
						}
						// 判断唯一性
						sameNode, err := GetSingleNodesByRoot(driver, ctx, "", "fileName", node.FileName)
						if err != nil {
							return false, err
						}
						if sameNode != nil {
							return false, errors.New("新文件fileName碰撞检测失败")
						}
						fileNameTemp = node.FileName
					}

					// 最后一个节点
					newNodeInfo = &dataModels.FileNode{
						IsDir:      node.IsDir,
						Name:       nodeName,
						UpdateTime: time.Now().Format("2006-01-02 15:04:05"),
						Type:       node.Type,
						Size:       node.Size,
						FileHash:   node.FileHash,
						FileName:   fileNameTemp,
						NodeType:   nodeTypeTemp,
					}
				}
				attributes := ReturnAttribute(*newNodeInfo)
				fmt.Println("attributes", attributes)
				cepherInsert += fmt.Sprintf("MERGE (%s_%s:NodeAttribute{%s})\n", nodeName, strconv.Itoa(index+1), attributes)
				if index > 0 {
					cepherConnections += fmt.Sprintf("MERGE (%s_%s)-[:CONNECTED_TO]->(%s_%s)\n", childTree[index-1], strconv.Itoa(index), nodeName, strconv.Itoa(index+1))
				} else {
					// 第一项， 源头是可插入目标元素
					cepherConnections += fmt.Sprintf("MERGE (root)-[:CONNECTED_TO]->(%s_%s)\n", nodeName, strconv.Itoa(index+1))
				}
			}
			cepherInsert += cepherConnections
			fmt.Println("cypoher", cepherInsert)
			_, err := tx.Run(
				ctx,
				cepherInsert,
				nil)
			if err != nil {
				fmt.Println("neo4j插入节点失败", err)
				// 如果提交失败，回滚事务
				if rErr := tx.Rollback(ctx); rErr != nil {
					return false, rErr
				}
				return false, err
			}
		} else {
			// 边界情况 说明此时是有一条完全一样的路径存 不做任何操作 如果是恢复->则报错告诉有相同路径
			if isResotre {
				return false, errors.New("叶节点存在相同名称文件")
			}
		}
	}
	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	/**
	  	新增节点
	    MERGE (u {type: 'user', name: 'devtest'})
	    ON CREATE SET
	      u.authority = 'xxx-auth',
	      u.userId = 'xxx-userId'
	    RETURN u
	*/
	return true, nil
}

// 上面的插入操作隔绝了插入租户的可能性， 所以租户要单独插入=> 一个租户只能单独给自己租户下面添加模块， 而插入节点的操作只允许admin可以,这里我制定了用户 admin就是根用户
//func InserUser(driver neo4j.DriverWithContext, ctx context.Context, agentName string) (isOk bool, err error) {
//	if ctx.Value("name") != "admin" {
//		return false, errors.New("无权限")
//	}
//	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
//	defer session.Close(ctx)
//	tx, err := session.BeginTransaction(ctx)
//	defer tx.Close(ctx)
//	if err != nil {
//		fmt.Println("事务开始失败")
//		return false, err
//	}
//	addAgentCepher := fmt.Sprintf("MERGE (user:NodeAttribute{name: \"%s\",updateTime: \"%s\",isDir: 1, type: \"directory\",size: 0, fileHash: \"\", fileName:\"remoteDisk\", nodeType: \"user\"})")
//	_, err := tx.Run(
//		ctx,
//		addRootCepher,
//		nil)
//	if err != nil {
//		fmt.Println("neo4j插入根节点失败")
//		// 如果提交失败，回滚事务
//		if rErr := tx.Rollback(ctx); rErr != nil {
//			return false, rErr
//		}
//		return false, err
//	}
//	return true, nil
//}

// 返回查询节点的及其一下的所有节点关系  参数可以根据固定的attribute查找节点(去除了（查找）根节点)， 如果单节点， 不构成树， 则什么都不返回
func GetAllNodesByRoot(driver neo4j.DriverWithContext, ctx context.Context, cepher string, attributeKey string, attributeValue string) (dataModels.NodesInfo, error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	if len(cepher) == 0 {
		cepher = fmt.Sprintf("MATCH p=(root:NodeAttribute {%s: \"%s\"})-[:CONNECTED_TO*]->(subtree:NodeAttribute) WHERE NOT (subtree)-[:CONNECTED_TO]->() RETURN p", attributeKey, attributeValue)
	}
	result, err := session.Run(ctx, cepher, nil)
	if err != nil {
		return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
	}
	pathNodeCacheMap := make(map[string]bool, 0)
	nodes := make([]*dataModels.FileNode, 0)
	paths := make([][]string, 0)
	pathNodeCacheMap[attributeValue] = true
	// 获取查询结果
	for result.Next(ctx) {
		record := result.Record()
		values := record.Values
		if err != nil {
			return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
		}
		if len(values) > 0 {
			path := values[0].(neo4j.Path)
			fmt.Println("node", path)
			pathNodes := path.Nodes
			pathCollect := make([]string, 0)
			for _, pathNode := range pathNodes {
				if !pathNodeCacheMap[pathNode.Props[attributeKey].(string)] {
					fileNode := &dataModels.FileNode{
						IsDir:      pathNode.Props["isDir"].(int64),
						Name:       pathNode.Props["name"].(string),
						UpdateTime: pathNode.Props["updateTime"].(string),
						Type:       pathNode.Props["type"].(string),
						Size:       pathNode.Props["size"].(int64),
						FileHash:   pathNode.Props["fileHash"].(string),
						FileName:   pathNode.Props["fileName"].(string),
						NodeType:   pathNode.Props["nodeType"].(string),
					}
					nodes = append(nodes, fileNode)
					pathNodeCacheMap[pathNode.Props[attributeKey].(string)] = true
				}
				pathCollect = append(pathCollect, pathNode.Props[attributeKey].(string))
			}
			paths = append(paths, pathCollect)

		}
	}
	return dataModels.NodesInfo{Nodes: nodes, Paths: paths}, err
}

// 返回查询节点的及其一下的所有节点关系  参数可以根据固定的attribute查找节点(包含（查找）根节点), 如果是单个节点， 没有路径， 仅仅返回这个节点
func GetAllNodesByRootWithRootNode(driver neo4j.DriverWithContext, ctx context.Context, cepher string, attributeKey string, attributeValue string) (dataModels.NodesInfo, error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	if len(cepher) == 0 {
		cepher = fmt.Sprintf("MATCH p=(root:NodeAttribute {%s: \"%s\"})-[:CONNECTED_TO*]->(subtree:NodeAttribute) WHERE NOT (subtree)-[:CONNECTED_TO]->() RETURN p", attributeKey, attributeValue)
	}
	result, err := session.Run(ctx, cepher, nil)
	if err != nil {
		return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
	}
	pathNodeCacheMap := make(map[string]bool, 0)
	nodes := make([]*dataModels.FileNode, 0)
	paths := make([][]string, 0)
	//pathNodeCacheMap[attributeValue] = true
	// 获取查询结果
	for result.Next(ctx) {
		record := result.Record()
		values := record.Values
		if err != nil {
			return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
		}
		if len(values) > 0 {
			path := values[0].(neo4j.Path)
			fmt.Println("node", path)
			pathNodes := path.Nodes
			pathCollect := make([]string, 0)
			for _, pathNode := range pathNodes {
				if !pathNodeCacheMap[pathNode.Props[attributeKey].(string)] {
					fileNode := &dataModels.FileNode{
						IsDir:      pathNode.Props["isDir"].(int64),
						Name:       pathNode.Props["name"].(string),
						UpdateTime: pathNode.Props["updateTime"].(string),
						Type:       pathNode.Props["type"].(string),
						Size:       pathNode.Props["size"].(int64),
						FileHash:   pathNode.Props["fileHash"].(string),
						FileName:   pathNode.Props["fileName"].(string),
						NodeType:   pathNode.Props["nodeType"].(string),
					}
					nodes = append(nodes, fileNode)
					pathNodeCacheMap[pathNode.Props[attributeKey].(string)] = true
				}
				pathCollect = append(pathCollect, pathNode.Props[attributeKey].(string))
			}
			paths = append(paths, pathCollect)

		}
	}
	if len(nodes) == 0 {
		cepherSingle := fmt.Sprintf("MATCH (root:NodeAttribute {%s: \"%s\"}) RETURN root", attributeKey, attributeValue)
		singleResult, err := session.Run(ctx, cepherSingle, nil)
		fmt.Println("执行单个节点查询", cepherSingle)
		if err != nil {
			return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
		}
		for singleResult.Next(ctx) {
			record := singleResult.Record()
			serachNodes := record.Values
			if err != nil {
				return dataModels.NodesInfo{Nodes: nil, Paths: nil}, err
			}
			if len(serachNodes) > 0 {
				for _, singleNode := range serachNodes {
					if !pathNodeCacheMap[singleNode.(neo4j.Node).Props[attributeKey].(string)] {
						fileNode := &dataModels.FileNode{
							IsDir:      singleNode.(neo4j.Node).Props["isDir"].(int64),
							Name:       singleNode.(neo4j.Node).Props["name"].(string),
							UpdateTime: singleNode.(neo4j.Node).Props["updateTime"].(string),
							Type:       singleNode.(neo4j.Node).Props["type"].(string),
							Size:       singleNode.(neo4j.Node).Props["size"].(int64),
							FileHash:   singleNode.(neo4j.Node).Props["fileHash"].(string),
							FileName:   singleNode.(neo4j.Node).Props["fileName"].(string),
							NodeType:   singleNode.(neo4j.Node).Props["nodeType"].(string),
						}
						nodes = append(nodes, fileNode)
						pathNodeCacheMap[singleNode.(neo4j.Node).Props[attributeKey].(string)] = true
					}
				}
			}
		}
	}
	return dataModels.NodesInfo{Nodes: nodes, Paths: paths}, err
}

// 在文件上传到minio前， 获取节点，做碰撞检测
// 返回单个节点的信息  insert 和 del在用 不校验 不暴露
func GetSingleNodesByRoot(driver neo4j.DriverWithContext, ctx context.Context, cepher string, attributeKey string, attributeValue string) (*dataModels.FileNode, error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	if len(cepher) == 0 {
		cepher = fmt.Sprintf("MATCH (n:NodeAttribute {%s: \"%s\"}) RETURN n", attributeKey, attributeValue)
	}
	result, err := session.Run(ctx, cepher, nil)
	if err != nil {
		return nil, err
	}
	res := make([]*dataModels.FileNode, 0)
	// 获取查询结果
	for result.Next(ctx) {
		record := result.Record()
		nodes := record.Values
		fmt.Println("输出结果", nodes)
		if len(nodes) > 0 {
			for _, node := range nodes {
				fileNode := &dataModels.FileNode{
					IsDir:      (node.(neo4j.Node)).Props["isDir"].(int64),
					Name:       (node.(neo4j.Node)).Props["name"].(string),
					UpdateTime: (node.(neo4j.Node)).Props["updateTime"].(string),
					Type:       (node.(neo4j.Node)).Props["type"].(string),
					Size:       (node.(neo4j.Node)).Props["size"].(int64),
					FileHash:   (node.(neo4j.Node)).Props["fileHash"].(string),
					FileName:   (node.(neo4j.Node)).Props["fileName"].(string),
					NodeType:   (node.(neo4j.Node)).Props["nodeType"].(string),
				}
				res = append(res, fileNode)
			}
		}
	}
	if res == nil || len(res) == 0 {
		return nil, nil
	}
	return res[0], nil
}

// 通过路径查找节点(做同层的节点碰撞检测： 检测条件(name相同)) 不校验， 暴露需要做好外部校验
func GetNodeWithPath(driver neo4j.DriverWithContext, ctx context.Context, nodes []string) (*dataModels.FileNode, error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	cypher := fmt.Sprintf("MATCH path = (%s_0:NodeAttribute { name: \"%s\"})\n", nodes[0], nodes[0])
	for index, nodeName := range nodes {
		if index != 0 {
			cypher += fmt.Sprintf("-[:CONNECTED_TO]->(%s_%s:NodeAttribute {name: \"%s\"})\n", nodeName, strconv.Itoa(index), nodeName)
		}
	}
	lastIndex := len(nodes) - 1
	lastSearchNodename := fmt.Sprintf("%s_%s", nodes[lastIndex], strconv.Itoa(lastIndex))
	cypher += fmt.Sprintf("RETURN %s", lastSearchNodename)
	fmt.Println("根据路径查找末端节点", cypher)
	result, err := session.Run(ctx, cypher, nil)
	if err != nil {
		return nil, err
	}
	res := make([]*dataModels.FileNode, 0)
	// 获取查询结果
	for result.Next(ctx) {
		record := result.Record()
		nodes := record.Values
		fmt.Println("输出结果", nodes)
		if len(nodes) > 0 {
			for _, node := range nodes {
				fileNode := &dataModels.FileNode{
					IsDir:      (node.(neo4j.Node)).Props["isDir"].(int64),
					Name:       (node.(neo4j.Node)).Props["name"].(string),
					UpdateTime: (node.(neo4j.Node)).Props["updateTime"].(string),
					Type:       (node.(neo4j.Node)).Props["type"].(string),
					Size:       (node.(neo4j.Node)).Props["size"].(int64),
					FileHash:   (node.(neo4j.Node)).Props["fileHash"].(string),
					FileName:   (node.(neo4j.Node)).Props["fileName"].(string),
					NodeType:   (node.(neo4j.Node)).Props["nodeType"].(string),
				}
				res = append(res, fileNode)
			}
		}
	}
	if res == nil || len(res) == 0 {
		return nil, nil
	}
	return res[0], nil
}

// 删除子树/节点
func DelSubTree(driver neo4j.DriverWithContext, ctx context.Context, node dataModels.InputFileInfo) (isOk bool, err error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	if node.FileName == "" {
		return false, errors.New("请输入要删除的节点")
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	// 开始事务
	tx, err := session.BeginTransaction(ctx)
	defer tx.Close(ctx)
	if err != nil {
		fmt.Println("事务开始失败")
		return false, err
	}

	// 1: 找到需要摘除的子树,子树的根节点的父节点
	cypherSearch := fmt.Sprintf("MATCH (parent)-[:CONNECTED_TO*1]->(child:NodeAttribute{fileName:\"%s\"})\nRETURN parent", node.FileName)
	results, err := tx.Run(
		ctx,
		cypherSearch,
		nil)
	if err != nil {
		return false, err
	}
	res := make([]*dataModels.FileNode, 0)
	for results.Next(ctx) {
		record := results.Record()
		nodes := record.Values
		if len(nodes) > 0 {
			for _, node := range nodes {
				fileNode := &dataModels.FileNode{
					IsDir:      (node.(neo4j.Node)).Props["isDir"].(int64),
					Name:       (node.(neo4j.Node)).Props["name"].(string),
					UpdateTime: (node.(neo4j.Node)).Props["updateTime"].(string),
					Type:       (node.(neo4j.Node)).Props["type"].(string),
					Size:       (node.(neo4j.Node)).Props["size"].(int64),
					FileHash:   (node.(neo4j.Node)).Props["fileHash"].(string),
					FileName:   (node.(neo4j.Node)).Props["fileName"].(string),
					NodeType:   (node.(neo4j.Node)).Props["nodeType"].(string),
				}
				res = append(res, fileNode)
			}
		}
	}
	if len(res) == 0 {
		return false, errors.New("未找到需要删除的节点的父节点")
	}
	subTreeRootParent := res[0]
	// 2第二步执行前： (重要)判断需要添加判断只允许删除用户自己的节点下面的文件 节点向上查找给出结果
	cypherGetUser := fmt.Sprintf("MATCH (root:NodeAttribute{name:\"remoteDisk\"})-[:CONNECTED_TO]->(a:NodeAttribute {name: \"%s\"})\nMATCH (b:NodeAttribute {fileName: \"%s\"})\nCALL apoc.path.subgraphNodes(b, {\n  relationshipFilter: 'CONNECTED_TO<', \n  depth: 999 \n})\nYIELD node\nWHERE node = a  // 检查是否找到a节点\nRETURN a", ctx.Value("name"), node.FileName)
	resultsUser, err := tx.Run(
		ctx,
		cypherGetUser,
		nil)
	if err != nil {
		return false, err
	}
	resUsers := make([]*dataModels.FileNode, 0)
	for resultsUser.Next(ctx) {
		record := resultsUser.Record()
		nodes := record.Values
		if len(nodes) > 0 {
			for _, node := range nodes {
				fileNode := &dataModels.FileNode{
					IsDir:      (node.(neo4j.Node)).Props["isDir"].(int64),
					Name:       (node.(neo4j.Node)).Props["name"].(string),
					UpdateTime: (node.(neo4j.Node)).Props["updateTime"].(string),
					Type:       (node.(neo4j.Node)).Props["type"].(string),
					Size:       (node.(neo4j.Node)).Props["size"].(int64),
					FileHash:   (node.(neo4j.Node)).Props["fileHash"].(string),
					FileName:   (node.(neo4j.Node)).Props["fileName"].(string),
					NodeType:   (node.(neo4j.Node)).Props["nodeType"].(string),
				}
				resUsers = append(resUsers, fileNode)
			}
		}
	}
	if len(resUsers) == 0 {
		return false, errors.New("操作节点越界,请切换到对应账户")
	}
	// 2: 在remoteDisk下面摘除，转接到trashDisk->租户下面
	// 删除关系
	cypherDeleteConnection := fmt.Sprintf("MATCH (parent:NodeAttribute{fileName: \"%s\"})-[r:CONNECTED_TO]->(delNode:NodeAttribute{fileName: \"%s\"})\nDELETE r\n", subTreeRootParent.FileName, node.FileName)
	// 提交
	_, err = tx.Run(
		ctx,
		cypherDeleteConnection,
		nil)
	if err != nil {
		// 如果提交失败，回滚事务
		if rErr := tx.Rollback(ctx); rErr != nil {
			return false, rErr
		}
		return false, err
	}
	// 重新建立关系
	// 判断是否有垃圾站
	rootNode, err := GetSingleNodesByRoot(driver, ctx, "", "fileName", "trashDisk")
	if err != nil {
		return false, err
	}
	newCypher := ""
	if rootNode == nil {
		// 如果垃圾站不存在就建立垃圾站
		newCypher += fmt.Sprintf("MERGE (root:NodeAttribute{name: \"trashDisk\",updateTime: \"2023011\",isDir: 1, type: \"directory\",size: 0, fileHash: \"\", fileName:\"trashDisk\", nodeType: \"root\"})\n")
	} else {
		newCypher += fmt.Sprintf("MATCH (root:NodeAttribute{fileName: \"trashDisk\"})\n")
	}
	// 先判断回收站中是否有该用户  前两层 的name是独一无二的， 其他层无法再现
	currentUserTrash := []string{"trashDisk", ctx.Value("name").(string)}
	userNode, err := GetNodeWithPath(driver, ctx, currentUserTrash)
	if err != nil {
		return false, err
	}
	if userNode == nil {
		// 没有租户先新建租户节点
		uniqueFileName, err := utils.GenerateUniqueString(ctx.Value("name").(string)+time.Now().Format("2006-01-02 15:04:05"), 50)
		if err != nil {
			return false, err
		}
		userNodeInfo := &dataModels.FileNode{
			IsDir:      1,
			Name:       ctx.Value("name").(string),
			UpdateTime: time.Now().Format("2006-01-02 15:04:05"),
			Type:       "directory",
			Size:       0,
			FileHash:   "",
			FileName:   uniqueFileName,
			NodeType:   "user",
		}
		userAttribute := ReturnAttribute(*userNodeInfo)
		newCypher += fmt.Sprintf("MERGE (user: NodeAttribute{%s})\n", userAttribute)
	} else {
		// 如果找到租户,则申明节点
		newCypher += fmt.Sprintf("MATCH (trashRoot: NodeAttribute{fileName:\"trashDisk\", nodeType: \"root\"})-[:CONNECTED_TO]->(user:NodeAttribute{name:\"%s\", nodeType:\"user\"})\n", ctx.Value("name"))
	}
	// 把树挂载到TrashDisk下面的[user name]下面
	cypherNewConnection := newCypher + fmt.Sprintf("\nWITH root,user\n MATCH (delTreeRootNode: NodeAttribute{fileName:\"%s\"})\n MERGE (root)-[:CONNECTED_TO]->(user)\nMERGE (user)-[:CONNECTED_TO]->(delTreeRootNode) \n", node.FileName)
	fmt.Println("cypher:", cypherNewConnection)
	// 提交
	_, err = tx.Run(
		ctx,
		cypherNewConnection,
		nil)
	if err != nil {
		// 如果提交失败，回滚事务
		if rErr := tx.Rollback(ctx); rErr != nil {
			return false, rErr
		}
		return false, err
	}
	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// 重新拼接节点： 1: 实现恢复(指定路径恢复) 2: 移动子树
func MoveSubTree(driver neo4j.DriverWithContext, ctx context.Context, sourceNodes []string, targetNodes []string) (isOk bool, err error) {
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	if len(sourceNodes) == 0 || len(targetNodes) == 0 {
		return false, errors.New("需要指定移动的目录和目标目录")
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	// 开始事务
	tx, err := session.BeginTransaction(ctx)
	defer tx.Close(ctx)
	if err != nil {
		fmt.Println("事务开始失败")
		return false, err
	}
	// 1 移动的原始路径和目标路径必须在权限允许范围内，才能移动
	if !utils.Contains(sourceNodes, ctx.Value("name").(string)) || !utils.Contains(sourceNodes, ctx.Value("name").(string)) {
		return false, errors.New("操作越界，请操作权限范围内的目录")
	}
	// 2: 判断原始路径是否有效
	sourceFileNode, err := GetNodeWithPath(driver, ctx, sourceNodes)
	if err != nil {
		return false, err
	}
	if sourceFileNode == nil {
		return false, errors.New("原始路径错误")
	}
	// 3：判断目标路径是否有效
	targetFileNode, err := GetNodeWithPath(driver, ctx, targetNodes)
	if err != nil {
		return false, err
	}
	if targetFileNode == nil {
		return false, errors.New("目标路径错误")
	}
	// 4： 删除原始路径关
	if sourceFileNode.FileName == "" {
		return false, errors.New("移动的原始目录不合法")
	}
	// 删除界节点，重新连接关系
	/**
	MATCH (sourcePreRootNode)-[r:CONNECTED_TO]->(sourceRootNode:NodeAttribute{fileName:"8fa6d1d1ae335746e948b03806a28db867802251040e8f4461"})
	MATCH (sourceNode:NodeAttribute{fileName:"8fa6d1d1ae335746e948b03806a28db867802251040e8f4461"})
	MATCH (targetNode:NodeAttribute{fileName:"0db5504d3c8cefdea09b18362bd81a79fb4f11ef47280ee4d7"})
	WITH  targetNode, sourceNode, r
	MERGE (targetNode)-[:CONNECTED_TO]->(sourceNode)
	DELETE r
	*/
	cypherMove := fmt.Sprintf("MATCH (sourcePreRootNode)-[r:CONNECTED_TO]->(sourceRootNode:NodeAttribute{fileName:\"%s\"})\nMATCH (sourceNode:NodeAttribute{fileName:\"%s\"})\nMATCH (targetNode:NodeAttribute{fileName:\"%s\"})\nWITH  targetNode, sourceNode, r\nMERGE (targetNode)-[:CONNECTED_TO]->(sourceNode)\nDELETE r", sourceFileNode.FileName, sourceFileNode.FileName, targetFileNode.FileName)
	// 提交
	_, err = tx.Run(
		ctx,
		cypherMove,
		nil)
	if err != nil {
		// 如果提交失败，回滚事务
		if rErr := tx.Rollback(ctx); rErr != nil {
			return false, rErr
		}
		return false, err
	}
	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// 根据节点向上查找路径， 顶点是remoteDisk或者trashDisk （这个查找不需要校验， 因为不会越界） 暂时功能使用
func GetUpNodesListBaseNode(driver neo4j.DriverWithContext, ctx context.Context, fileName string) (nodesInfo *dataModels.NodesInfo, err error) {
	/**
	MATCH (root:NodeAttribute{name:"remoteDisk"})
	MATCH (b:NodeAttribute {fileName: '0db5504d3c8cefdea09b18362bd81a79fb4f11ef47280ee4d7'})  // 匹配节点 trash dir2
	CALL apoc.path.subgraphNodes(b, {
	  relationshipFilter: 'CONNECTED_TO<',  // 向上查找关系
	  depth: 999  // 设置足够大的深度以确保可以找到根节点
	})
	YIELD node
	RETURN node;
	*/
	//sessionMutex.Lock()
	//defer sessionMutex.Unlock()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	cepher := fmt.Sprintf("MATCH (root:NodeAttribute{name:\"remoteDisk\"})\n\t\tMATCH (b:NodeAttribute {fileName: \"%s\"}) \n CALL apoc.path.subgraphNodes(b, {\n relationshipFilter: 'CONNECTED_TO<', \n  depth: 999 \n})\nYIELD node\nRETURN node;", fileName)
	result, err := session.Run(ctx, cepher, nil)
	if err != nil {
		return nil, err
	}
	res := make([]*dataModels.FileNode, 0)
	// 获取查询结果
	for result.Next(ctx) {
		record := result.Record()
		nodes := record.Values
		if len(nodes) > 0 {
			for _, node := range nodes {
				fmt.Println("nodes---------------", node)
				fileNode := &dataModels.FileNode{
					IsDir:      (node.(neo4j.Node)).Props["isDir"].(int64),
					Name:       (node.(neo4j.Node)).Props["name"].(string),
					UpdateTime: (node.(neo4j.Node)).Props["updateTime"].(string),
					Type:       (node.(neo4j.Node)).Props["type"].(string),
					Size:       (node.(neo4j.Node)).Props["size"].(int64),
					FileHash:   (node.(neo4j.Node)).Props["fileHash"].(string),
					FileName:   (node.(neo4j.Node)).Props["fileName"].(string),
					NodeType:   (node.(neo4j.Node)).Props["nodeType"].(string),
				}
				res = append(res, fileNode)
			}
		}
	}
	return &dataModels.NodesInfo{
		Nodes: res,
	}, nil
}

/**
 查找所有叶子节点的路径
MATCH p=(root:Node {id: 3})-[:CONNECTED_TO*]->(subtree:Node)
WHERE NOT (subtree)-[:CONNECTED_TO]->()
RETURN p
*/

/**
后续用到的测试案例:
	1： 测试添加的路径：想要存储的目标路径假如是文件夹，则插入无效
	2： 存在脏数据的情况， 假如脏数据，节点，非叶子节点是文件， 则这条路径下面的所有插入操作将无效
*/
