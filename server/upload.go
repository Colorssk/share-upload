package server

import (
	"bytes"
	"context"
	"file-upload/common"
	"file-upload/dataModels"
	rabbitmq "file-upload/mq"
	"file-upload/repositories"
	"file-upload/util"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/pkg/errors"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type IUploadService interface {
	// 上传文件到指定目录
	UploadFiles(context.Context, string, multipart.File, *multipart.FileHeader, string) (bool, error)
	// 校验存放的目标路径是否合法
	CheckTargetPathValid(context.Context, string) (bool, error)
	// 图数据库的插入
	// relativePath string  完整的相对路径(包含租户+租户下面的存储路径)
	AddNode(context.Context, string) (bool, error)
	// 图数据库的查询
	// nodeId string 需要查找的节点id
	SearchNodes(context.Context, string) ([]*dataModels.FileNode, error)
	// 图数据库的修改节点(目前只需要修改时间/名称)
	EditNode(context.Context, dataModels.FileNode) (bool, error)
	// 删除节点及其子节点
	RemoveNode(context.Context, dataModels.FileNode) (bool, error)

	// 测试neo4j的临时方法
	TestNeo4j(context.Context, string) (dataModels.NodesInfo, error)
	TestNeo4j2(context.Context, string) (*dataModels.FileNode, error)
	TestPostNeo4j(context.Context, dataModels.InputFileInfo) (bool, error)
	TestPostMoveNeo4j(context.Context, []string, []string) (bool, error)
	TestNeo4UpSearch(context.Context, string) (*dataModels.NodesInfo, error)

	// 正式 服务
	UpdateFileStructure(context.Context, dataModels.InputFileInfo) (bool, error)
	SaveFileHash(context.Context, dataModels.FileInfo) (bool, error)
	GetFileHashCrash(context.Context, string) (bool, error)
	CombineFile(context.Context, string, []string, string, string) error
	StartCombine(context.Context, string, string, string) error
	PureAddNode(context.Context, dataModels.InputFileInfo) (bool, error)
	//文件下载
	DownloadFiles(context.Context, string) (*bytes.Buffer, string, error)
}

type UploadService struct {
	Neo4jClient neo4j.DriverWithContext
	minioClient *util.Minio
	uploadDao   repositories.IUploadDao
	mq          *rabbitmq.RabbitMQ
}

func NewUploadService(neo4jClick neo4j.DriverWithContext, minio *util.Minio, uploadDao repositories.IUploadDao, mq *rabbitmq.RabbitMQ) IUploadService {
	return &UploadService{Neo4jClient: neo4jClick, minioClient: minio, uploadDao: uploadDao, mq: mq}
}

// 上传单个文件到指定目录
func (u *UploadService) UploadFiles(ctx context.Context, bucketName string, fileReader multipart.File, fileHeader *multipart.FileHeader, fileFullName string) (bool, error) {
	if bucketName == "" {
		bucketName = "remote-disk"
	}
	err := u.minioClient.UploadFile(ctx, bucketName, fileHeader.Filename, fileReader, fileFullName)
	if err != nil {
		return false, err
	}
	return true, err
}

func (u *UploadService) CheckTargetPathValid(ctx context.Context, targetPath string) (bool, error) {
	return true, nil
}

// 图数据库的插入
// relativePath string  完整的相对路径(包含租户+租户下面的存储路径)
func (u *UploadService) AddNode(context.Context, string) (bool, error) {
	return true, nil
}

// 图数据库的查询
// nodeId string 需要查找的节点id
func (u *UploadService) SearchNodes(context.Context, string) ([]*dataModels.FileNode, error) {
	return nil, nil
}

// 图数据库的修改节点(目前只需要修改时间/名称)
func (u *UploadService) EditNode(context.Context, dataModels.FileNode) (bool, error) {
	return true, nil
}

// 删除节点及其子节点
func (u *UploadService) RemoveNode(context.Context, dataModels.FileNode) (bool, error) {
	return true, nil
}

// 测试neO4j
func (u *UploadService) TestNeo4j(ctx context.Context, value string) (NodesInfo dataModels.NodesInfo, err error) {
	res, err := common.GetAllNodesByRootWithRootNode(u.Neo4jClient, ctx, "", "fileName", value)
	return res, err
}

// 测试neo4j向上查找
func (u *UploadService) TestNeo4UpSearch(ctx context.Context, fileName string) (NodesInfo *dataModels.NodesInfo, err error) {
	res, err := common.GetUpNodesListBaseNode(u.Neo4jClient, ctx, fileName)
	return res, err
}

// 根据节点路径查找节点参数中末端节点中的信息, 这里仅仅用于查找二级和二级以下单节点信息
func (u *UploadService) TestNeo4j2(ctx context.Context, value string) (NodesInfo *dataModels.FileNode, err error) {
	//res, err := common.GetSingleNodesByRoot(u.Neo4jClient, ctx, "", "fileName", value)
	paramValues := strings.Split(value, ",")
	// 拥有最高权限的admin不管
	if ctx.Value("name").(string) == "admin" || util.ContainsTemp(paramValues, ctx.Value("name").(string)) {
		NodesInfo, err = common.GetNodeWithPath(u.Neo4jClient, ctx, paramValues)
		//if NodesInfo == nil {
		//	return nil, nil
		//}
	} else {
		err = errors.New("无权限")
	}
	return
}

func (u *UploadService) TestPostNeo4j(ctx context.Context, insedrtNodeInfo dataModels.InputFileInfo) (isOk bool, err error) {
	res, err := common.InsertNodes(u.Neo4jClient, ctx, "", insedrtNodeInfo, false)
	//res, err := common.DelSubTree(u.Neo4jClient, ctx, insedrtNodeInfo)
	return res, err
}

// server: 节点新增(单纯的文件夹新增)
func (u *UploadService) PureAddNode(ctx context.Context, insertNodeInfo dataModels.InputFileInfo) (isOk bool, err error) {
	res, err := common.InsertNodes(u.Neo4jClient, ctx, "", insertNodeInfo, false)
	return res, err
}

func (u *UploadService) TestPostMoveNeo4j(ctx context.Context, sourceNodes []string, targetNodes []string) (isOk bool, err error) {
	res, err := common.MoveSubTree(u.Neo4jClient, ctx, sourceNodes, targetNodes)
	return res, err
}

// 接口服务： 记录文件结构
func (u *UploadService) UpdateFileStructure(ctx context.Context, insedrtNodeInfo dataModels.InputFileInfo) (isOk bool, err error) {
	res, err := common.InsertNodes(u.Neo4jClient, ctx, "", insedrtNodeInfo, false)
	return res, err
}

// 持久化存储filehash
func (u *UploadService) SaveFileHash(ctx context.Context, newFile dataModels.FileInfo) (bool, error) {
	isOk, err := u.uploadDao.AddFile(newFile)
	if err != nil || !isOk {
		return false, err
	}
	return true, nil
}

// 用于filehash碰撞
func (u *UploadService) GetFileHashCrash(ctx context.Context, fileHashInTentant string) (bool, error) {
	filInfo, err := u.uploadDao.GetFileInfo(fileHashInTentant)
	if err == nil && filInfo == nil {
		return true, nil
	} else {
		return false, err
	}
}

// 根据文件列表读取文件， 然后存储到
func (u *UploadService) CombineFile(ctx context.Context, userName string, fileList []string, fileFullName string, fileType string) error {
	// 读取minio数据， 然后合并， 最后写入
	return u.minioClient.CombineFile(ctx, "block-disk", "remote-disk", userName, fileList, fileFullName, fileType)
}

// 发起合并
func (u *UploadService) StartCombine(ctx context.Context, message string, uniqueFileName string, fileType string) error {
	messageWithFileName := fmt.Sprintf("%v&%v@%v", message, uniqueFileName, fileType)
	return u.mq.PublishWorkQueue(messageWithFileName)
}

//  下载文件

func (u *UploadService) DownloadFiles(ctx context.Context, nodeName string) (*bytes.Buffer, string, error) {
	var resTree dataModels.FileWithParDirPath
	nodeFileRecrodList := make(map[string]bool, 0)
	if nodeName != "" {
		// 1: 获取路径下面的所有文件列表(一维)并且包含对应的父容器路径, 和文件夹树（n维，唯一根），文件夹树如果不存在，代表当前的是获取的单个文件，如果文件列表长度大于一，直接返回异常
		// 根据节点先获取节点树
		res, err := common.GetAllNodesByRootWithRootNode(u.Neo4jClient, ctx, "", "fileName", nodeName)
		if len(res.Paths) > 0 {
			// 获取根节点
			// 每个paths都是包含叶子节点的完整路径
			for _, paths := range res.Paths {
				// 迭代每一条路径，构建树结构
				if len(paths) > 0 {
					for pathIndex, nodeFileName := range paths {
						if len(res.Nodes) > 0 {
							for _, singleNode := range res.Nodes {
								// 找到节点, 并且节点位插入过
								if singleNode.FileName == nodeFileName && !nodeFileRecrodList[nodeFileName] {
									nodeFileRecrodList[nodeFileName] = true
									nodeInfo := singleNode
									// 压入节点树
									if resTree.FileNodeInfo.FileName == "" {
										//未有根节点
										resTree.FileNodeInfo = *nodeInfo
									} else {
										if pathIndex != 0 {
											// 子节点
											util.IterateTree(&resTree, *nodeInfo, paths[pathIndex-1])
										}
									}
								}
							}
						}
					}
				}
			}
		} else {
			// 代表是单独的文件或者文件夹
		}
		if err != nil {
			return nil, "", nil
		}

		// 2: 本地构建文件夹树
		// (1): 为每个下载任务建一个hash目录， 防碰撞
		rootFileName := util.SetUniqueString("")
		// 下载到本地
		rootLocalPath := filepath.Join("./", rootFileName)
		bucketName := "remote-disk"
		var wg sync.WaitGroup
		fmt.Println("xxx", rootLocalPath)
		err = util.CreateFilesLocal(ctx, u.minioClient, rootLocalPath, &resTree, bucketName, &wg)
		if err != nil {
			fmt.Println("down load to local err:", err)
			return nil, "", err
		}
		wg.Wait()
		// 3: 对本地文件打包成zip 传输回去
		//zipFile := fmt.Sprintf("%s.tar.gz", rootFileName)
		resBuf, err := util.GetZipFromLocal(rootLocalPath)
		if err != nil {
			fmt.Println("压缩获取buff 失败")
			return nil, "", err
		}

		// 4 打包完成之后，最后做一下本地目录清空
		err = os.RemoveAll(rootLocalPath)
		if err != nil {
			fmt.Println("删除文件夹失败:", err)
		}
		return resBuf, rootFileName, nil
	} else {
		return nil, "", errors.New("请选择目录")
	}
}
