package dataModels

import (
	"bytes"
	"mime/multipart"
)

type File struct {
	ID         int64  `json:"id" sql:"id" req:"id"`
	ProjectId  int64  `json:"projectId" sql:"projectId" req:"projectId"`
	Name       string `json:"name" sql:"name" req:"name"`
	Path       string `json:"path" sql:"path" req:"path"`
	LinkFields string `json:"linkFields" sql:"linkFields" req:"linkFields"`
}

//type Chunk struct {
//	Filehash   string `json:"filehash"`
//	UploadAt   string `json:"uploadAt"`
//	ChunkCount int64  `json:"chunkCount"`
//}

type FileNode struct {
	Id         int64  `json:"id"`
	IsDir      int64  `json:"isDir"`      // 是否文件夹: 1 是 2 不是
	Name       string `json:"name"`       // 节点名称
	UpdateTime string `json:"updateTime"` // 更新时间
	Type       string `json:"type"`       // 文件夹类型
	Size       int64  `json:"size"`       // 文件大小，基础单位: MB
	FileHash   string `json:"fileHash"`   // 完整文件的hash值
	FileName   string `json:"fileName"`   //  文件名称(hash，同时作为碰撞检测)
	NodeType   string `json:"nodeType"`   // 节点类型 root/user/normal 甚于类型都属于游离类型
}

type NodesInfo struct {
	Nodes []*FileNode `json:"nodes"`
	Paths [][]string  `json:"paths"`
}

type InputFileInfo struct {
	IsDir        int64  `json:"isDir"`        // 是否文件夹: 1 是 2 不是
	Name         string `json:"name"`         // 节点名称  会用来生成unique的节点名称-> 给到fileName
	UpdateTime   string `json:"updateTime"`   // 更新时间
	Type         string `json:"type"`         // 节点类型  文件夹统一是directory
	Size         int64  `json:"size"`         // 文件大小，基础单位: MB
	FileHash     string `json:"fileHash"`     // 完整文件的hash值（相同的文件内容，输出相同的唯一识别码）
	FileName     string `json:"fileName"`     //  文件名称(hash，同时作为碰撞检测)  在插入操作中：这个字段是由Name转化来的，如果文件类型，那么就是由Name+Type转化来的
	TargetPath   string `json:"targetPath"`   // 目标存储路径
	RelativePath string `json:"relativePath"` // 相对存储路径  包含上面的那么  用于记录文件结构的，不用于文件存储  路径这块不能随便填写，因为作为name的路径查找，如果随便填，插入和查询都会异常， 路径的最后一个节点，建议最好和name一样
	NodeType     string `json:"nodeType"`     // 节点类型 root/user/normal 甚于类型都属于游离类型
}

type InputMoveRequest struct {
	SourceNodes []string `json:"sourceNodes"`
	TargetNodes []string `json:"targetNodes"`
}

type FileUploadRequest struct {
	FileHeader     *multipart.FileHeader `form:"fileHeader"`
	FileData       multipart.File        `form:"fileData"`
	UploadFileInfo InputFileInfo         `form:"uploadFileInfo"`
	// 必要的元数据基础信息
	CurrentCount int64 `json:"currentCount"` // 当前区块排序， 从1开始
	ChunkCount   int64 `json:"chunkCount"`   // 记录的是分块的总数
	// 标志位是否是分块信息
	IsBlock bool `json:"isBlock"`
}

// 数据库 file 表 记录上传的文件内容的hash
type FileInfo struct {
	FileHashInTenant string `form:"fileHashInTenant" sql:"fileHashInTenant"`
	Path             string `form:"path" sql:"path"`
}

type DowloadRequest struct {
	NodeName string `form:"nodeName" sql:"nodeName"`
}

// 节点树
type FileWithParDirPath struct {
	FileNodeInfo      FileNode             `form:"fileNodeInfo" sql:"fileNodeInfo"`
	ChildFileNodeInfo []FileWithParDirPath `form:"childFileNodeInfo" sql:"childFileNodeInfo"`
}

// downloadTask
type DownLoadTask struct {
	Node     *FileWithParDirPath `form:"node" sql:"node"`
	FilePath string              `form:"filePath" sql:"filePath"`
}

type FileResp struct {
	Data     *bytes.Buffer
	FileName string
}

// 插入的节点信息
type InsertRequest struct {
	Path string `form: "fi"`
}
