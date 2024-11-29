package http

import (
	"context"
	"encoding/json"
	"file-upload/dataModels"
	"file-upload/endpoint"
	"file-upload/server"
	"file-upload/util"
	"fmt"
	go_kit_endpoint "github.com/go-kit/kit/endpoint"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

func decodeSetMaxSizeRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.SetMaxSizeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeGetMaxSizeRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	return nil, nil
}

func decodeSetValidTypesRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.SetValidFileTypeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeGetValidTypesRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	return nil, nil
}

func decodeSetThresHoldRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.SetThresHoldRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeGetThresHoldRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	return nil, nil
}

func decodeSetBlockSizeRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.SetBlockSizeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeGetBlockSizeRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	return nil, nil
}

func decodeSetMaxBreaksRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request endpoint.SetMaxBreaksRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeGetMaxBreaksRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	return nil, nil
}

// 上传接口参数解析
//func decodeUploadRequest(ctx context.Context, r *http.Request) (interface{}, error) {
//	var request endpoint.UploadRequest
//	chunkNum := r.FormValue("chunkNumber")
//	chunkData := r.FormValue("chunkData")
//	return nil, nil
//}

// 测试neo4j接口
func decodeTestNeo4jRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	// 从请求URL中获取参数
	// 从请求的 URL 中获取查询参数
	queryParams := r.URL.Query()

	// 获取名为 "paramName" 的参数值
	paramValue := queryParams.Get("value")
	request := endpoint.TestNeo4jRequest{
		Value: paramValue,
	}
	return request, nil
}

// post neo4j 测试接口
func decodeTestPostNeo4jRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request dataModels.InputFileInfo
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// post move neo4j 测试接口
func decodeTestMovePostNeo4jRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request dataModels.InputMoveRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

// 文件下载参数
func decodeGetDownloadFilesRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	queryParams := r.URL.Query()
	fmt.Println("参数获取 download")
	// 获取名为 "nodeName" 的参数值
	nodeName := queryParams.Get("nodeName")
	request := dataModels.DowloadRequest{
		NodeName: nodeName,
	}
	return request, nil
}

// 文件上传
func decodeUploadFilesRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request dataModels.FileUploadRequest
	// formdata post
	err := r.ParseMultipartForm(32 << 20) // 32MB大小限制
	if err != nil {
		return nil, err
	}
	request.FileData, request.FileHeader, err = r.FormFile("file")
	chunkCount, err := strconv.ParseInt(r.FormValue("chunkCount"), 10, 64)
	if err != nil {
		return nil, err
	}
	request.ChunkCount = chunkCount
	isBlock, err := strconv.ParseBool(r.FormValue("isBlock"))
	if err != nil {
		return nil, err
	}
	request.IsBlock = isBlock
	currentCount, err := strconv.ParseInt(r.FormValue("currentCount"), 10, 64)
	if err != nil {
		return nil, err
	}
	request.CurrentCount = currentCount
	// 单独的uploadFileInfo结构
	isDir, err := strconv.ParseInt(r.FormValue("isDir"), 10, 64)
	size, err := strconv.ParseInt(r.FormValue("size"), 10, 64)
	receiveFileInfo := dataModels.InputFileInfo{
		IsDir:        isDir,
		Name:         r.FormValue("name"),
		UpdateTime:   r.FormValue("updateTime"),
		Type:         r.FormValue("type"),
		Size:         size,
		FileHash:     r.FormValue("fileHash"),
		FileName:     r.FormValue("fileName"),
		TargetPath:   r.FormValue("targetPath"),
		RelativePath: r.FormValue("relativePath"),
		NodeType:     "",
	}
	request.UploadFileInfo = receiveFileInfo
	return request, nil
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	return json.NewEncoder(w).Encode(response)
}

// 返回的数据直接二进制流
func encodeBinResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	rep := response.(dataModels.FileResp)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "inline; filename="+rep.FileName)
	_, err := w.Write(rep.Data.Bytes())
	return err
}

// 包裹中间件
func WrapWithAuth(newEndpoint go_kit_endpoint.Endpoint) go_kit_endpoint.Endpoint {
	return endpoint.AuthMiddleware()(newEndpoint)
}
func NewHTTPServer(svc server.ISettingSerive, svUpload server.IUploadService) http.Handler {
	SetMaxSizeHandle := httptransport.NewServer(
		endpoint.SetMaxSizeEndpoint(svc),
		decodeSetMaxSizeRequest,
		encodeResponse,
	)
	GetMaxSizeHandle := httptransport.NewServer(
		endpoint.GetMaxSizeEndpoint(svc),
		decodeGetMaxSizeRequest,
		encodeResponse,
	)
	SetValidFileTypeHandle := httptransport.NewServer(
		endpoint.SetValidFileTypeEndpoint(svc),
		decodeSetValidTypesRequest,
		encodeResponse,
	)
	GetValidFileTypeHandle := httptransport.NewServer(
		endpoint.GetValidFileTypeEndpoint(svc),
		decodeGetValidTypesRequest,
		encodeResponse,
	)
	SetThresHoldHandle := httptransport.NewServer(
		endpoint.SetThresHoldEndpoint(svc),
		decodeSetThresHoldRequest,
		encodeResponse,
	)
	GetThresHoldHandle := httptransport.NewServer(
		endpoint.GetThresHoldEndpoint(svc),
		decodeGetThresHoldRequest,
		encodeResponse,
	)
	SetBlockSizeHandle := httptransport.NewServer(
		endpoint.SetBlockSizeEndpoint(svc),
		decodeSetBlockSizeRequest,
		encodeResponse,
	)
	GetBlockSizeHandle := httptransport.NewServer(
		endpoint.GetBlockSizeEndpoint(svc),
		decodeGetBlockSizeRequest,
		encodeResponse,
	)
	SetMaxBreaksHandle := httptransport.NewServer(
		endpoint.SetMaxBreaksEndpoint(svc),
		decodeSetMaxBreaksRequest,
		encodeResponse,
	)
	GetMaxBreaksHandle := httptransport.NewServer(
		endpoint.GetMaxBreaksEndpoint(svc),
		decodeGetMaxBreaksRequest,
		encodeResponse,
	)
	uploadEndpointSvc := endpoint.UploadEndpoint(svUpload)
	uploadEndpointSvc = endpoint.AuthMiddleware()(uploadEndpointSvc)
	// 上传 http
	uploadHandle := httptransport.NewServer(
		uploadEndpointSvc,
		decodeUploadFilesRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)

	// 新增节点 http
	//insertEndpointSvc := endpoint.InsertEndpointSvc(svUpload)
	//insertEndpointSvc = endpoint.AuthMiddleware()(insertEndpointSvc)
	//insertNodeHandle := httptransport.NewServer(
	//	insertEndpointSvc,
	//	decodeUploadFilesRequest,
	//	encodeResponse,
	//	httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
	//		ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
	//		ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
	//		return ctx
	//	}),
	//)

	// 获取节点树
	TestNeo4jHandle := httptransport.NewServer(
		WrapWithAuth(endpoint.TestNeo4jEndpoint(svUpload)),
		decodeTestNeo4jRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			fmt.Println("获取到的请求头", request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)

	TestUpSearchNeo4jHandle := httptransport.NewServer(
		endpoint.TestSearchUpNeo4jEndpoint(svUpload),
		decodeTestNeo4jRequest,
		encodeResponse,
	)

	TestNeo42jHandle := httptransport.NewServer(
		WrapWithAuth(endpoint.TestNeo4j2Endpoint(svUpload)),
		decodeTestNeo4jRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			fmt.Println("获取到的请求头", request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)

	TestPostNeo4jHandle := httptransport.NewServer(
		WrapWithAuth(endpoint.TestPostNeo4jEndpoint(svUpload)),
		decodeTestPostNeo4jRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			fmt.Println("获取到的请求头", request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)

	TestMoveNeo4jHandle := httptransport.NewServer(
		WrapWithAuth(endpoint.TestPostMoveNeo4jEndpoint(svUpload)),
		decodeTestMovePostNeo4jRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			fmt.Println("获取到的请求头", request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)

	// 当前文件列表  参数(父节点) 根据父节点拿出来所有信息
	//listHandle := httptransport.NewServer(
	//	endpoint.GetMaxBreaksEndpoint(svUpload),
	//	decodeGetMaxBreaksRequest,
	//	encodeResponse,
	//)

	DownloadHandle := httptransport.NewServer(
		WrapWithAuth(endpoint.GetDownloadFilesEndpoint(svUpload)),
		decodeGetDownloadFilesRequest,
		encodeBinResponse,
		httptransport.ServerBefore(func(ctx context.Context, request *http.Request) context.Context {
			fmt.Println("DownloadHandle 获取到的请求头", request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, util.JWT_CONTEXT_KEY, request.Header.Get("Authorization"))
			ctx = context.WithValue(ctx, "UserId", request.Header.Get("UserId"))
			return ctx
		}),
	)
	r := mux.NewRouter()
	r.Handle("/set-max-size", SetMaxSizeHandle).Methods("POST")
	r.Handle("/get-max-size", GetMaxSizeHandle).Methods("GET")
	r.Handle("/set-valid-types", SetValidFileTypeHandle).Methods("POST")
	r.Handle("/get-valid-types", GetValidFileTypeHandle).Methods("GET")
	r.Handle("/set-thresHold", SetThresHoldHandle).Methods("POST")
	r.Handle("/get-thresHold", GetThresHoldHandle).Methods("GET")
	r.Handle("/set-block-size", SetBlockSizeHandle).Methods("POST")
	r.Handle("/get-block-size", GetBlockSizeHandle).Methods("GET")
	r.Handle("/set-max-breaks", SetMaxBreaksHandle).Methods("POST")
	r.Handle("/get-max-breaks", GetMaxBreaksHandle).Methods("GET")
	// 接收上传的分块信息
	r.Handle("/upload", uploadHandle).Methods("POST")
	// 新增非文件节点
	//r.Handle("/insert", insertNodeHandle).Methods("POST")
	// 查询当前租户的文件信息
	//r.Handle("/file-list", listHandle).Methods("GET")
	//  测试neo4j
	r.Handle("/get-tree", TestNeo4jHandle).Methods("GET")
	r.Handle("/get-node", TestNeo42jHandle).Methods("GET")
	r.Handle("/test/upSearch", TestUpSearchNeo4jHandle).Methods("GET")
	// post 测试neo4j
	r.Handle("/insert", TestPostNeo4jHandle).Methods("POST")
	// post 测试neto4节点移动
	r.Handle("/move/test", TestMoveNeo4jHandle).Methods("POST")

	// 文件下载
	r.Handle("/downlaod", DownloadHandle).Methods("GET")
	return r
}
