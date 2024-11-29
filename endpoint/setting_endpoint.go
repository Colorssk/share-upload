package endpoint

import (
	"context"
	"errors"
	"file-upload/dataModels"
	"file-upload/server"
	"file-upload/util"
	utils "file-upload/util"
	"fmt"
	"github.com/go-kit/kit/endpoint"
	"path/filepath"
	"strconv"
	"strings"
)

type CommonError struct {
	ErrorInfo *string
	Response  interface{}
}

type SetMaxSizeRequest struct {
	MaxSize float64 `json:"maxSize"`
}

type SetValidFileTypeRequest struct {
	ValidTypes []string `json:"validTypes"`
}

type SetThresHoldRequest struct {
	ThresHold float64 `json:"thresHold"`
}

type SetBlockSizeRequest struct {
	BlockSize float64 `json:"blockSize"`
}

type SetMaxBreaksRequest struct {
	MaxBreaks int64 `json:"maxBreaks"`
}

// 中间件校验的返回结果
type AuthResponse struct {
	Status  int64  `json:"status"`
	Message string `json:"message"`
}

func handleCommonResp(res interface{}, err error) (interface{}, error) {
	if err != nil {
		errorMsg := err.Error()
		return &CommonError{ErrorInfo: &errorMsg, Response: nil}, nil
	}
	return &CommonError{ErrorInfo: nil, Response: res}, nil
}

// 设置文件最大值
func SetMaxSizeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(SetMaxSizeRequest)
		res, err := svc.SetMaxSize(ctx, req.MaxSize)
		return handleCommonResp(res, err)
	}
}
func GetMaxSizeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		res, err := svc.GetMaxSize(ctx)
		return handleCommonResp(res, err)
	}
}

// 可上传类型
func SetValidFileTypeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(SetValidFileTypeRequest)
		res, err := svc.SetValidFileType(ctx, req.ValidTypes)
		return handleCommonResp(res, err)
	}
}
func GetValidFileTypeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		res, err := svc.GetValidFileType(ctx)
		return handleCommonResp(res, err)
	}
}

// 分块阈值
func SetThresHoldEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(SetThresHoldRequest)
		res, err := svc.SetThresHold(ctx, req.ThresHold)
		return handleCommonResp(res, err)
	}
}
func GetThresHoldEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		res, err := svc.GetThreHold(ctx)
		return handleCommonResp(res, err)
	}
}

// 单独区块大小
func SetBlockSizeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(SetBlockSizeRequest)
		res, err := svc.SetBlockSize(ctx, req.BlockSize)
		return handleCommonResp(res, err)
	}
}
func GetBlockSizeEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		res, err := svc.GetBlockSize(ctx)
		return handleCommonResp(res, err)
	}
}

// 并行最大的上传数目(最大断点数目)
func SetMaxBreaksEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(SetMaxBreaksRequest)
		res, err := svc.SetMaxBreaks(ctx, req.MaxBreaks)
		return handleCommonResp(res, err)
	}
}
func GetMaxBreaksEndpoint(svc server.ISettingSerive) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		res, err := svc.GetMaxBreaks(ctx)
		return handleCommonResp(res, err)
	}
}

// 上传接口
func UploadEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(dataModels.FileUploadRequest)
		// 判断是否秒传
		uniqueFileHash, err := util.CalculateSHA256(req.FileData)
		// 分辨区块和文件的唯一识别码(分块和文件都用这个做识别码)
		fileHashInTentant := fmt.Sprintf("%s_%s", ctx.Value("name").(string), uniqueFileHash)
		if err != nil {
			return handleCommonResp(nil, errors.New("生成文件内容hash失败"))
		}
		// 1: 持久化数据库匹配， 如果存在,则文件系统中不要存储(租户+文件内容hash)，秒传不变换节点层级
		isCrash, err := svc.GetFileHashCrash(ctx, fileHashInTentant)
		if err != nil {
			return handleCommonResp(nil, err)
		}
		if !isCrash {
			fmt.Println("秒传生效")
			// 存在相同文件  即使分块信息， 对于相同的文件，也会显示直接完成(判断标准加上以用户为前提的)， 下面逻辑直接跳过
			return handleCommonResp("上传成功", nil)
		}
		// 一定要有文件数据
		if req.FileData != nil {
			// 不能不传文件名称
			if len(req.UploadFileInfo.Name) == 0 {
				return handleCommonResp(nil, errors.New("未设置文件名称"))
			}
			// 这个名称具有唯一性
			uniqueFileName, err := utils.GenerateUniqueString(req.UploadFileInfo.Name+uniqueFileHash, 50)
			if err != nil {
				return handleCommonResp(nil, errors.New("请上传数据"))
			}
			resultChan := make(chan struct {
				isOk bool
				err  error
			})

			// 判断当前文件是否是分块逻辑
			if req.IsBlock {
				// 如果没有设置分块数目，抛出异常
				if req.ChunkCount == 0 || req.CurrentCount == 0 {
					return handleCommonResp(nil, errors.New("请确认总上传的文件分块数或者确认当前分块号是否有效"))
				}
				// 对于上传的多个区块一定要有公共(对于区块来说)并且唯一(对于不同文件来说)的识别码
				if req.UploadFileInfo.FileHash == "" {
					return handleCommonResp(nil, errors.New("未找到区块信息的唯一识别码"))
				}
				// 走分块逻辑， 还需要合并, 默认是批量上传
				// 3: 消息中间件发送消息，告知文件合并
				go func() {
					//区块的唯一识别码：每个区块的字段filehash 需要一致代表，是整个文件整体的唯一识别码
					uniqeBlockGroupUnicode := fmt.Sprintf("%s_%s", ctx.Value("name").(string), req.UploadFileInfo.FileHash)
					// 1: 上传到分块区域
					isOk, err := svc.UploadFiles(ctx, "block-disk", req.FileData, req.FileHeader, uniqueFileName)
					if err != nil || !isOk {
						resultChan <- struct {
							isOk bool
							err  error
						}{false, err}
					}
					// 2: 记录成功上传的分块文件的文件名
					fileNameInMinio := uniqueFileName + filepath.Ext(req.FileHeader.Filename)
					// 对于上传的分块，在上传文件系统之后， 然后就先在redis中记录，然后判断是否满足合并
					// 存储redisvalue值: minio中的文件名+_+区块号, 存储的key值: 租户+_+区块唯一识别码
					fileNameInMinioWithCount := fmt.Sprintf("%s_%s", fileNameInMinio, strconv.FormatInt(req.CurrentCount, 10))
					fmt.Println("设置redis分块id", uniqeBlockGroupUnicode)
					// *存 没有满足合并条件，记录上传区块信息到redis
					err = util.SetBlockInfo(ctx, uniqeBlockGroupUnicode, fileNameInMinioWithCount)
					if err != nil {
						resultChan <- struct {
							isOk bool
							err  error
						}{false, err}
					}
					// *取 查找是否满足合并条件 key： 是文件内容的识别码
					fmt.Println("uniqeBlockGroupUnicode", uniqeBlockGroupUnicode)
					blockFileNames, err := util.CheckCanCombineAndReturn(ctx, uniqeBlockGroupUnicode, req.ChunkCount)
					if err != nil {
						resultChan <- struct {
							isOk bool
							err  error
						}{false, err}
					}
					if blockFileNames != nil {
						// 满足了合并条件
						// 发起合并任务
						// 合并消息格式: 租户|fileName_1|fileName_2....
						messageList := make([]string, 0)
						nameStr := fmt.Sprintf("%v", ctx.Value("name"))
						messageList = append(messageList, nameStr)
						for _, blockFileNameInMinio := range *blockFileNames {
							messageList = append(messageList, blockFileNameInMinio)
						}
						publishMessage := strings.Join(messageList, "|")
						fmt.Println("开发发送异步合并消息")
						// rabbitmq 发起合并消息
						err = svc.StartCombine(ctx, publishMessage, fileNameInMinio, req.UploadFileInfo.Type)
						if err != nil {
							resultChan <- struct {
								isOk bool
								err  error
							}{false, err}
						}
						fmt.Println("此时应该要结束----发送消息成功")
						// 请求发送成功之后删除redis中记录的数据  对于发起合并的redis记录， 可以直接删除
						util.DelInfo(ctx, uniqeBlockGroupUnicode)
						// 先记录节点 合并的操作异步处理完成
						newFileNodeWithUniqueFileName := dataModels.InputFileInfo{
							IsDir:        req.UploadFileInfo.IsDir,
							Name:         req.UploadFileInfo.Name,
							UpdateTime:   req.UploadFileInfo.UpdateTime,
							Type:         req.UploadFileInfo.Type,
							Size:         req.UploadFileInfo.Size,
							FileHash:     req.UploadFileInfo.FileHash,
							FileName:     fileNameInMinio,
							TargetPath:   req.UploadFileInfo.TargetPath,
							RelativePath: req.UploadFileInfo.RelativePath,
							NodeType:     req.UploadFileInfo.NodeType,
						}
						isUpdateFileStructure, err := svc.UpdateFileStructure(ctx, newFileNodeWithUniqueFileName)
						if err != nil || !isUpdateFileStructure {
							resultChan <- struct {
								isOk bool
								err  error
							}{false, err}
						}
					}
					// 如果不满足合并，分块的上传已经完成， 可以执行最后的记录任务， 持久化记录在数据库中
					resultChan <- struct {
						isOk bool
						err  error
					}{true, nil}
				}()
			} else {
				go func() {
					// 整块文件上传, 上传就算完成
					isOk, err := svc.UploadFiles(ctx, "", req.FileData, req.FileHeader, uniqueFileName)
					if err != nil || !isOk {
						resultChan <- struct {
							isOk bool
							err  error
						}{false, err}
					}
					// 对于上传成功的文件，进行记录节点信息
					fmt.Println("记录节点信息")
					newFileName := uniqueFileName + filepath.Ext(req.FileHeader.Filename)
					newFileNodeWithUniqueFileName := dataModels.InputFileInfo{
						IsDir:        req.UploadFileInfo.IsDir,
						Name:         req.UploadFileInfo.Name,
						UpdateTime:   req.UploadFileInfo.UpdateTime,
						Type:         req.UploadFileInfo.Type,
						Size:         req.UploadFileInfo.Size,
						FileHash:     req.UploadFileInfo.FileHash,
						FileName:     newFileName,
						TargetPath:   req.UploadFileInfo.TargetPath,
						RelativePath: req.UploadFileInfo.RelativePath,
						NodeType:     req.UploadFileInfo.NodeType,
					}
					isUpdateFileStructure, err := svc.UpdateFileStructure(ctx, newFileNodeWithUniqueFileName)
					if err != nil || !isUpdateFileStructure {
						resultChan <- struct {
							isOk bool
							err  error
						}{false, err}
					}

					resultChan <- struct {
						isOk bool
						err  error
					}{true, nil}
				}()
			}
			// 等待goroutine处理结果
			result := <-resultChan

			if !result.isOk || result.err != nil {
				return handleCommonResp("文件上传失败", result.err)
			}
		} else {
			return handleCommonResp(nil, errors.New("请上传数据"))
		}
		// 到这就是上传成功了,然后把文件内容hash的记录存储在持久化数据库中
		newFile := dataModels.FileInfo{
			FileHashInTenant: fileHashInTentant,
		}
		saveOk, err := svc.SaveFileHash(ctx, newFile)
		if err != nil || !saveOk {
			return handleCommonResp(nil, errors.New("文件信息存储失败"))
		}
		return handleCommonResp("上传成功", nil)
	}

}

// 身份校验的中间件 不调用consul直接走一遍parse校验就行了
func AuthMiddleware() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			token := fmt.Sprint(ctx.Value(util.JWT_CONTEXT_KEY))
			fmt.Sprintln("走到这里了", ctx.Value(util.JWT_CONTEXT_KEY))
			if token == "" {
				return AuthResponse{Status: dataModels.AuthFailure, Message: "请登录!"}, nil
			}
			// 校验
			claims, err := utils.ParseToken(token)
			// parse中已经校验token是否合法或者过期
			if err != nil {
				return AuthResponse{Status: dataModels.AuthFailure, Message: "请登录!"}, err
			}
			// 校验token是否已经注销
			tokenID, ok := claims["TokenId"].(string)
			if !ok {
				return AuthResponse{Status: dataModels.AuthFailure, Message: "请登录!"}, err
			}
			isRevoked, err := utils.GetCache(tokenID)
			if err != nil {
				return AuthResponse{Status: dataModels.AuthFailure, Message: "请登录!"}, err
			}
			if isRevoked {
				// 已经注销了
				return AuthResponse{Status: dataModels.AuthFailure, Message: "请登录!"}, err
			} else {
				// 最后token信息， 压入上下文
				userName, ok := claims["Name"].(string)
				if !ok {
					return AuthResponse{Status: dataModels.AuthFailure, Message: "!"}, errors.New("用户名获取失败")
				}
				ctx = context.WithValue(ctx, "name", userName)
				return next(ctx, request)
			}

		}
	}
}

type TestNeo4jRequest struct {
	Value string `json:"value"`
}

func TestNeo4jEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(TestNeo4jRequest)
		res, err := svc.TestNeo4j(ctx, req.Value)
		return handleCommonResp(res, err)
	}
}

func TestSearchUpNeo4jEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(TestNeo4jRequest)
		res, err := svc.TestNeo4UpSearch(ctx, req.Value)
		return handleCommonResp(res, err)
	}
}

func TestNeo4j2Endpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(TestNeo4jRequest)
		res, err := svc.TestNeo4j2(ctx, req.Value)
		return handleCommonResp(res, err)
	}
}

func TestPostNeo4jEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(dataModels.InputFileInfo)
		res, err := svc.TestPostNeo4j(ctx, req)
		return handleCommonResp(res, err)
	}
}

func TestPostMoveNeo4jEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(dataModels.InputMoveRequest)
		res, err := svc.TestPostMoveNeo4j(ctx, req.SourceNodes, req.TargetNodes)
		return handleCommonResp(res, err)
	}
}

func GetDownloadFilesEndpoint(svc server.IUploadService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(dataModels.DowloadRequest)
		res, fileName, err := svc.DownloadFiles(ctx, req.NodeName)
		if err != nil {
			fmt.Println("下载错误:", err)
		}
		return dataModels.FileResp{
			Data:     res,
			FileName: fileName,
		}, err
	}
}

// 新建节点 (纯文件夹)
//func InsertEndpointSvc(svc server.IUploadService) endpoint.Endpoint {
//	return func(ctx context.Context, request interface{}) (interface{}, error) {
//		req := request.(dataModels.InsertRequest)
//		res, err := svc.TestPostMoveNeo4j(ctx, req.SourceNodes, req.TargetNodes)
//		return handleCommonResp(res, err)
//	}
//}
