package util

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"file-upload/config"
	"file-upload/dataModels"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go/log"
	"gopkg.in/ini.v1"
	"hash"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cfg *ini.File

type Sha1Stream struct {
	_sha1 hash.Hash
}

func (obj *Sha1Stream) Update(data []byte) {
	if obj._sha1 == nil {
		obj._sha1 = sha1.New()
	}
	obj._sha1.Write(data)
}

func (obj *Sha1Stream) Sum() string {
	return hex.EncodeToString(obj._sha1.Sum([]byte("")))
}

func Sha1(data []byte) string {
	_sha1 := sha1.New()
	_sha1.Write(data)
	return hex.EncodeToString(_sha1.Sum([]byte("")))
}

func FileSha1(file *os.File) string {
	_sha1 := sha1.New()
	io.Copy(_sha1, file)
	return hex.EncodeToString(_sha1.Sum(nil))
}

func MD5(data []byte) string {
	_md5 := md5.New()
	_md5.Write(data)
	return hex.EncodeToString(_md5.Sum([]byte("")))
}

func FileMD5(file *os.File) string {
	_md5 := md5.New()
	io.Copy(_md5, file)
	return hex.EncodeToString(_md5.Sum(nil))
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}

func StructToMap(input interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	val := reflect.ValueOf(input)

	if val.Kind() == reflect.Struct {
		typ := val.Type()
		for i := 0; i < val.NumField(); i++ {
			fieldName := typ.Field(i).Tag.Get("json")
			fieldValue := val.Field(i).Interface()
			result[fieldName] = fieldValue
		}
	}

	return result
}

var RedisClient *redis.Client
var mutex sync.Mutex

func InitRedis() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Error(err)
	}
	if err != nil {
		fmt.Println("redis初始化失败", err.Error())
		panic(err)
	}
	// Initialize the Redis client with appropriate configuration
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     cfg.Section("redis").Key("uri").String(),      // Replace with your Redis server address
		Password: cfg.Section("redis").Key("password").String(), // Set password if applicable
		DB:       0,                                             // Use default database
	})
}

// true 代表能够获取到(已经被注销) false 没有获取到(没有被注销)
func GetCache(tokenId string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if RedisClient == nil {
		fmt.Println("只初始化redis一次")
		InitRedis()
	}
	// 从redis读取数据
	cachedData, err := RedisClient.Get(context.Background(), tokenId).Result()
	if err == nil {
		if len(cachedData) > 0 {
			return true, nil
		} else {
			return false, nil
		}
	} else {
		if errors.Is(err, redis.Nil) {
			// 键不存在
			return false, nil
		} else {
			fmt.Println("读取redis数据失败", err)
			return false, err
		}
	}
}

// 存储到redis中
func SetCache(tokenId string) (err error) {
	if RedisClient == nil {
		fmt.Println("只初始化redis一次")
		InitRedis()
	}
	if err := RedisClient.Set(context.Background(), tokenId, true, 0).Err(); err != nil {
		return err
	}
	return nil
}

// 分块信息存储到redis中
func SetBlockInfo(ctx context.Context, key string, value string) (err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if RedisClient == nil {
		fmt.Println("只初始化redis一次")
		InitRedis()
	}
	if err := RedisClient.RPush(ctx, key, value).Err(); err != nil {
		return err
	}
	return nil
}

// 删除数据
func DelInfo(ctx context.Context, key string) (err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if RedisClient == nil {
		fmt.Println("只初始化redis一次")
		InitRedis()
	}
	if err := RedisClient.Del(ctx, key).Err(); err != nil {
		return err
	}
	return nil
}

// 校验分块信息是否满足合并条件, 如果满足就返回数组， 如果不满足就返回空
func CheckCanCombineAndReturn(ctx context.Context, key string, maxCount int64) (blockFileNames *[]string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if RedisClient == nil {
		fmt.Println("只初始化redis一次")
		InitRedis()
	}
	// 读取list
	values, err := RedisClient.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if values == nil {
		return nil, nil
	}
	fmt.Println("查看是否分块完成-------------", int64(len(values)), maxCount)
	if int64(len(values)) == maxCount {
		resNames := &[]string{}
		*resNames = values
		return resNames, nil
	}
	return nil, nil
}

func GenerateUniqueString(input string, length int) (string, error) {
	// 创建SHA-256哈希器
	hasher := sha256.New()

	// 写入输入数据
	_, err := io.WriteString(hasher, input)
	if err != nil {
		return "", err
	}

	// 计算哈希值
	hashBytes := hasher.Sum(nil)

	// 将哈希值截断为所需长度
	if length > len(hashBytes)*2 {
		return "", fmt.Errorf("指定的长度过长")
	}

	hashString := hex.EncodeToString(hashBytes)[:length]
	return hashString, nil
}

func Contains(arr []string, target string) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}
	return false
}

// 泛型判断
func ContainsTemp[T comparable](slice []T, item T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func CalculateSHA256(file multipart.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func ExtractDigit(v string) (number int64) {
	list := strings.Split(v, "_")
	if len(list) != 2 {
		return 0
	}
	number, err := strconv.ParseInt(list[1], 10, 64)
	if err != nil {
		return 0
	}
	return number
}
func QuickSort(arr []string) []string {
	if len(arr) <= 1 {
		return arr
	}
	pivot := arr[0]
	var left, right []string

	for _, v := range arr[1:] {
		vValue := ExtractDigit(v)
		pivotValue := ExtractDigit(pivot)
		if vValue == 0 || pivotValue == 0 {
			continue
		}
		if vValue < pivotValue {
			left = append(left, v)
		} else {
			right = append(right, v)
		}
	}

	left = QuickSort(left)
	right = QuickSort(right)

	return append(append(left, pivot), right...)
}

// 节点树构建
func IterateTree(curNode *dataModels.FileWithParDirPath, nodeInfo dataModels.FileNode, parentNodeFileName string) {
	// 找到了需要插入的父节点
	if curNode.FileNodeInfo.FileName == parentNodeFileName {
		newNode := dataModels.FileWithParDirPath{
			FileNodeInfo:      nodeInfo,
			ChildFileNodeInfo: []dataModels.FileWithParDirPath{},
		}
		curNode.ChildFileNodeInfo = append(curNode.ChildFileNodeInfo, newNode)
	} else if len(curNode.ChildFileNodeInfo) > 0 {
		for i := range curNode.ChildFileNodeInfo {
			IterateTree(&curNode.ChildFileNodeInfo[i], nodeInfo, parentNodeFileName)
		}
	}

}

// 生成16长度的hash字符串
func SetUniqueString(s string) string {
	if s == "" {
		s = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	hashStr := sha256.New()
	hashStr.Write([]byte(s))
	res := hashStr.Sum(nil)
	return hex.EncodeToString(res)[:16]
}

func CreateDir(path string) error {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}
	return nil
}
func CrateDirIFNotExists(path string) error {
	fmt.Println("检查的路径", path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

func ProcessNode(ctx context.Context, client *Minio, bucketetName string, objectFilePath string, localFileFullPath string) error {
	//errCh := make(chan error, 1)
	//go func(innerCtx context.Context, innerClient *Minio, innerBucketetName string, innerObjectFilePath string, innerLocalFileFullPath string, innerWg *sync.WaitGroup) {
	//	defer innerWg.Done()
	//	err := innerClient.GetFileToLocal(innerCtx, innerBucketetName, innerObjectFilePath, innerLocalFileFullPath)
	//	if err != nil {
	//		errCh <- err
	//	}
	//}(ctx, client, bucketetName, objectFilePath, localFileFullPath, wg)
	//select {
	//case err := <-errCh: // 从 channel 中接收错误
	//	return err // 如果有错误，返回错误
	//case <-ctx.Done(): // 如果 context 被取消，返回 context 的错误
	//	fmt.Println("ctx 被取消")
	//	return ctx.Err()
	//}

	return client.GetFileToLocal(ctx, bucketetName, objectFilePath, localFileFullPath)
}

// tree 构建本地文件  parPathInMino: 是username下面的完整 mino中的路径（因为设计就是remote-disk 下面是租户，租户下面是所有的文件）
func CreateFilesLocal(ctx context.Context, client *Minio, parRootNamePath string, root *dataModels.FileWithParDirPath, bucketName string, wg *sync.WaitGroup) error {
	if ctx.Value("name").(string) == "" {
		// 如果没有用户名称， 就没必要走下去了
		return errors.New("unable to get any user info")
	}
	// 本地文件夹路径
	parPath := filepath.ToSlash(parRootNamePath)
	// 上层检点存在检查, 不存在自动新建 =》 所以递归方法的前提是不能是文件
	err := CrateDirIFNotExists(parPath)
	if err != nil {
		return err
	}
	// 继续 parRootNamePath 下面的文件夹/文件创建
	if root.FileNodeInfo.IsDir == 1 {
		// 当前目录的检查和新建
		curDirPath := filepath.Join(parPath, root.FileNodeInfo.Name)
		err := CrateDirIFNotExists(curDirPath)
		if err != nil {
			return err
		}
		// 当前节点是文件夹
		if len(root.ChildFileNodeInfo) > 0 {
			// 如果是文件夹直接进入递归
			fmt.Println("当前文件夹遍历次数:", len(root.ChildFileNodeInfo))
			for _, child := range root.ChildFileNodeInfo {
				if child.FileNodeInfo.IsDir == 1 {
					// 当前节点的子文件夹
					fmt.Println("当前节点的子文件夹，上层节点路径", curDirPath)
					err = CreateFilesLocal(ctx, client, curDirPath, &child, bucketName, wg)
					if err != nil {
						return err
					}
				} else {
					fmt.Println("当前文件处理", child.FileNodeInfo.Name)
					// 文件 代表是 叶子节点, 支持并发获取文件
					leafFilePathInMinio := filepath.ToSlash(filepath.Join(ctx.Value("name").(string), child.FileNodeInfo.FileName))

					extensions, err := mime.ExtensionsByType(child.FileNodeInfo.Type)
					if err != nil {
						return err
					}
					if len(extensions) == 0 {
						fmt.Println("该 MIME 类型没有对应的扩展名")
						return errors.New("未找到文件类型")
					}
					defaultExtension := extensions[0]
					fileName := child.FileNodeInfo.Name + defaultExtension
					curFileFullPath := filepath.ToSlash(filepath.Join(curDirPath, fileName))
					fmt.Println("nextParPath---------", curFileFullPath)
					// 获取和下载文件
					wg.Add(1)
					go func() {
						defer wg.Done()
						err = ProcessNode(ctx, client, bucketName, leafFilePathInMinio, curFileFullPath)
						if err != nil {
							fmt.Println("下载文件失败:", err)
						}
					}()
				}
			}
		}
		// 否则什么也不做 => 对于空文件夹来说， 下载下来也没有， 直接舍弃就行, 不想舍弃在这里创建文件夹
	} else {
		// 不是文件夹， 就是文件
		// 本地本地路径
		fullFullPath := filepath.ToSlash(parPath)
		// 查找的minio object 路径
		objectFilePath := filepath.ToSlash(filepath.Join(ctx.Value("name").(string), root.FileNodeInfo.FileName))
		wg.Add(1)
		fmt.Println("fullFullPath---------", fullFullPath)
		go func() {
			defer wg.Done()
			wg.Add(1)
			err = ProcessNode(ctx, client, bucketName, objectFilePath, fullFullPath)
			if err != nil {
				fmt.Println("下载文件失败:", err)
			}
		}()
	}
	return nil
}

// 从本地文件夹， 获取到zip文件
func GetZipFromLocal(sourceDir string) (*bytes.Buffer, error) {
	// 创建删除的缓存数据
	buf := new(bytes.Buffer)

	// 创建 gzip.Writer
	gzWriter := gzip.NewWriter(buf)
	defer gzWriter.Close()

	// 创建 tar.Writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	// 递归遍历源目录
	err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 获取文件信息
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// 修改 header 中的名称为相对路径
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		// 写入 header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// 如果是文件则写入文件内容
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			if _, err := io.Copy(tarWriter, file); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	fmt.Printf("输出打包之后的buff信息 '%v'\n", buf.Len())
	return buf, nil
}
