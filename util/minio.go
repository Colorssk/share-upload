package util

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Minio is a struct to wrap minio client
type Minio struct {
	Client *minio.Client
	mu     sync.RWMutex // 读写锁
}

// NewMinio is a function to create a new Minio struct
func NewMinio(endpoint, accessKey, secretKey string, secure bool) (*Minio, error) {
	// Initialize minio client object.
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}

	return &Minio{Client: client}, nil
}

// UploadFolder uploads a local folder to the specified bucket in minio.
func (m *Minio) UploadFolder(ctx context.Context, bucketName, folderPath, minioPrefix string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Check if bucket exists and create if it doesn't exist
	found, err := m.Client.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}
	if !found {
		// 设置存储桶的访问控制列表 ACL
		policy := fmt.Sprintf(`{
			"Version":"2012-10-17",
			"Statement":[
				{
					"Action":["s3:GetObject"],
					"Effect":"Allow",
					"Principal":{"AWS":["*"]},
					"Resource":["arn:aws:s3:::%s/*"],
					"Sid":"",
					"Condition": {
						 "StringLike": {
							"aws:Referer": "*"
						}
					}
				}
			]
		}`, bucketName)
		err = m.Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
		err = m.Client.SetBucketPolicy(ctx, bucketName, policy)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	strArr := strings.Split(filepath.ToSlash(folderPath), "/")
	fileName := strArr[1]
	fmt.Println("fileName", fileName)

	// Walk through local folder and upload all files
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk through local folder: %v", err)
		}

		// Ignore directories
		if info.IsDir() {
			return nil
		}

		// Prepare object name in MinIO
		objectName := filepath.ToSlash(strings.Replace(path, fileName, minioPrefix, 1))

		// Open file for reading
		//file, err := os.Open(path)
		//if err != nil {
		//	return fmt.Errorf("failed to open file %s: %v", path, err)
		//}
		//defer file.Close()

		var contentType string
		// 获取文件扩展名
		ext := filepath.Ext(path)
		fmt.Println("walk: path:", ext)
		if ext == "" {
			// 如果扩展名为空，使用默认的MIME类型
			contentType = "application/octet-stream"
		} else {
			// 根据扩展名获取MIME类型
			contentType := mime.TypeByExtension(ext)
			if contentType == "" {
				// 如果无法获取MIME类型，使用默认的MIME类型
				contentType = "application/octet-stream"
			}
		}
		fmt.Println("walk: extesion:", contentType)
		fmt.Println("work objectName", objectName)
		// Upload object to MinIO
		_, err = m.Client.FPutObject(ctx, bucketName, objectName, path, minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err != nil {
			return fmt.Errorf("failed to upload object %s to MinIO: %v", objectName, err)
		}

		return nil
	})

	return nil
}

// 传入数据,按照路径写入mino（对于分块数据存储需要存储在特定的文件夹下面: blockData, 其他整块文件都是平级存储，不存在深层层级结构(没有子目录)）
// bucketName: remoteDisk  objectName: 租户  提交文件 fileName 是独一无二的fileName
func (m *Minio) UploadFile(ctx context.Context, bucketName string, fileFullName string, fileReader multipart.File, uniqueFileName string) (err error) {
	// Check if bucket exists and create if it doesn't exist
	found, err := m.Client.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}
	if !found {
		// 设置存储桶的访问控制列表 ACL
		policy := fmt.Sprintf(`{
			"Version":"2012-10-17",
			"Statement":[
				{
					"Action":["s3:GetObject"],
					"Effect":"Allow",
					"Principal":{"AWS":["*"]},
					"Resource":["arn:aws:s3:::%s/*"],
					"Sid":"",
					"Condition": {
						 "StringLike": {
							"aws:Referer": "*"
						}
					}
				}
			]
		}`, bucketName)
		err = m.Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
		err = m.Client.SetBucketPolicy(ctx, bucketName, policy)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	var contentType string
	ext := filepath.Ext(fileFullName)
	if ext == "" {
		// 如果扩展名为空，使用默认的MIME类型
		contentType = "application/octet-stream"
	} else {
		// 根据扩展名获取MIME类型
		contentType = mime.TypeByExtension(ext)
		if contentType == "" {
			// 如果无法获取MIME类型，使用默认的MIME类型
			contentType = "application/octet-stream"
		}
	}
	newFileName := uniqueFileName + filepath.Ext(fileFullName)
	_, err = fileReader.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to upload object %s to MinIO: %v", err)
	}
	// 存储在租户下面
	objectName := filepath.ToSlash(filepath.Join(ctx.Value("name").(string), newFileName))
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err = m.Client.PutObject(ctx, bucketName, objectName, fileReader, -1, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to upload object %s to MinIO: %v", objectName, err)
	}

	return nil
}

// 读取多个文件然后拼接成一个新的数据流
/**
srcDir: 需要读取的文件目录，
fileNameList： 原始路径下文件名称列表 [fileName_1, fileName_2....]
dest: 需要合并之后推送到的目标路径
*/
func (m *Minio) CombineFile(ctx context.Context, blockBucketName string, bucketName string, userName string, fileNameList []string, fileFullName string, fileType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 先对文件列表进行排序
	sortedDileNameList := QuickSort(fileNameList)
	var mergedStream bytes.Buffer
	var firstBlockName string
	var count int
	for _, fileNameWithCount := range sortedDileNameList {
		fileNameList := strings.Split(fileNameWithCount, "_")
		if len(fileNameList) != 2 {
			fmt.Println("数据类型错误")
			break
		}
		fileName := fileNameList[0]
		firstBlockName = fileNameList[0]
		fileName = filepath.ToSlash(filepath.Join(userName, fileName))
		err := func(ctxInner context.Context, mergedStreamIner *bytes.Buffer, bucketNameInner string, fileNameInner string) error {
			object, err := m.Client.GetObject(ctxInner, bucketNameInner, fileNameInner, minio.GetObjectOptions{})
			objectReader := bufio.NewReader(object)
			defer object.Close()
			if err != nil {
				fmt.Println("Error get object", err)
				return err
			}
			bufferSize := 5 * 1024 * 1024 // 64KB 缓冲区大小
			buffer := make([]byte, bufferSize)
			startTime := time.Now()
			// 调用函数
			_, err = io.CopyBuffer(mergedStreamIner, objectReader, buffer)
			if err != nil {
				fmt.Println("Copy error", err)
				return err
			}
			endTime := time.Now()
			// 计算代码执行时长
			duration := endTime.Sub(startTime)
			fmt.Println("读写时长", duration)

			//// 设置文件流的读取位置为开头
			//_, err = object.Seek(0, io.SeekStart)
			//if err != nil {
			//	fmt.Println("Error seek object", err)
			//	return err
			//}
			//// 输出一些调试信息
			//objectInfo, err := object.Stat()
			//fmt.Println("Object metadata:", objectInfo.Size)
			//fmt.Println("mergedStreamIner length:", mergedStreamIner.Len())
			//startTime := time.Now()
			//// 调用函数
			//err = sequentialCopy(ctx, object, mergedStreamIner)
			//endTime := time.Now()
			//// 计算代码执行时长
			//duration := endTime.Sub(startTime)
			//fmt.Println("读写时长", duration)
			//if err != nil {
			//	// 处理错误的逻辑
			//}
			//if err != nil {
			//	fmt.Println("Error copying buffer to mergedStream:", err)
			//	return err
			//}
			return nil
		}(ctx, &mergedStream, blockBucketName, fileName)
		if err != nil {
			break
		}
		count++
	}
	if count != len(sortedDileNameList) {
		return errors.New("文件读写错误")
	}
	// Check if bucket exists and create if it doesn't exist
	found, err := m.Client.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}
	if !found {
		// 设置存储桶的访问控制列表 ACL
		policy := fmt.Sprintf(`{
			"Version":"2012-10-17",
			"Statement":[
				{
					"Action":["s3:GetObject"],
					"Effect":"Allow",
					"Principal":{"AWS":["*"]},
					"Resource":["arn:aws:s3:::%s/*"],
					"Sid":"",
					"Condition": {
						 "StringLike": {
							"aws:Referer": "*"
						}
					}
				}
			]
		}`, bucketName)
		err = m.Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
		err = m.Client.SetBucketPolicy(ctx, bucketName, policy)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	var contentType string
	ext := filepath.Ext(firstBlockName)
	fmt.Println("ext:", ext, mime.TypeByExtension(ext), fileType)
	if ext == "" {
		// 如果扩展名为空，使用默认的MIME类型
		if fileType != "" {
			contentType = fileType
		} else {
			contentType = "application/octet-stream"
		}

	} else {
		// 根据扩展名获取MIME类型
		contentType = mime.TypeByExtension(ext)
		if contentType == "" {
			// 如果无法获取MIME类型，使用默认的MIME类型
			contentType = "application/octet-stream"
		}
	}
	if err != nil {
		fmt.Println("failed to upload object to MinIO", err)
		return err
	}
	// 存储在租户下面
	objectName := filepath.ToSlash(filepath.Join(userName, fileFullName))
	mergedReader := bytes.NewReader(mergedStream.Bytes())
	_, err = m.Client.PutObject(ctx, bucketName, objectName, mergedReader, -1, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		fmt.Println("failed to upload object to MinIO", err)
		return err
	}

	return nil
}

// 获取单个的文件filePath 已经是bucket 文件夹下面的全路径
func (m *Minio) GetFileToLocal(ctx context.Context, bucketName string, filepath string, dsPath string) error {
	var infoBuffer bytes.Buffer
	object, err := m.Client.GetObject(ctx, bucketName, filepath, minio.GetObjectOptions{})
	objectReader := bufio.NewReader(object)
	defer object.Close()
	if err != nil {
		fmt.Println("Error get object", err)
		return err
	}
	bufferSize := 5 * 1024 * 1024 // 缓冲区大小
	buffer := make([]byte, bufferSize)
	startTime := time.Now()
	// 调用函数
	_, err = io.CopyBuffer(&infoBuffer, objectReader, buffer)
	if err != nil {
		fmt.Println("Copy error", err)
		return err
	}

	endTime := time.Now()
	// 计算代码执行时长
	duration := endTime.Sub(startTime)
	fmt.Println("读写时长", duration)
	fmt.Println("读写路径", dsPath)
	err = os.WriteFile(dsPath, infoBuffer.Bytes(), 0755)
	if err != nil {
		return err
	}
	return nil
}

func sequentialCopy(ctx context.Context, object *minio.Object, mergedStreamIner *bytes.Buffer) error {
	bufferSize := 1024 * 1024 // 1MB 缓冲区大小
	chunk := make([]byte, bufferSize)

	for {
		n, err := object.Read(chunk)
		if err != nil && err != io.EOF {
			fmt.Println("Error reading from object:", err)
			return err
		}
		if n == 0 {
			break
		}

		// 顺序写入数据
		_, err = mergedStreamIner.Write(chunk[:n])
		if err != nil {
			fmt.Println("Error writing to mergedStreamIner:", err)
			return err
		}
	}

	return nil
}
