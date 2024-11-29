package main

import (
	"context"
	"errors"
	"file-upload/common"
	"file-upload/config"
	rabbitmq "file-upload/mq"
	"file-upload/repositories"
	"file-upload/server"
	httpTransport "file-upload/transport/http"
	"file-upload/util"
	"flag"
	"fmt"
	_ "github.com/mbobakov/grpc-consul-resolver"
	"github.com/opentracing/opentracing-go/log"
	"golang.org/x/sync/errgroup"
	"strings"

	//"google.golang.org/grpc"
	//"google.golang.org/grpc/credentials/insecure"
	"net"
	"net/http"
)

var (
	httpAddr = flag.String("http-addr", ":18188", "文件上传服务")
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 在主函数结束时取消context
	g, ctx := errgroup.WithContext(ctx)
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Error(err)
	}
	// 获取consul连接
	//target := fmt.Sprintf("consul://%s/%s?wait=14s", cfg.Section("consul").Key("uri").String(), cfg.Section("consul").Key("service").String())
	//conn, err := grpc.Dial(
	//	//consul网络必须是通的   user_srv表示服务 wait:超时 tag是consul的tag  可以不填
	//	target,
	//	grpc.WithTransportCredentials(insecure.NewCredentials()),
	//	//轮询法   必须这样写   grpc在向consul发起请求时会遵循轮询法
	//	grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`),
	//)
	//if err != nil {
	//	log.Error(err)
	//}
	//defer conn.Close()
	// minio连接
	minioClient, errMinio := util.NewMinio(cfg.Section("minio").Key("uri").String(), cfg.Section("minio").Key("accessKey").String(), cfg.Section("minio").Key("secretKey").String(), false)
	if errMinio != nil {
		fmt.Println("error")
		log.Error(errMinio)
	}
	// neo4j获取实例
	neo4jDb, err := common.NewNeo4JConn(ctx, cfg)
	defer neo4jDb.Close(ctx)
	if err != nil {
		fmt.Println("error", err)
	}
	dbAuth, errDbAuth := common.NewMysqlConn(cfg, "fileAssets")
	if errDbAuth != nil {
		fmt.Println("error")
		log.Error(errDbAuth)
	}
	// 处理合并消息 rabbitmq
	mq := rabbitmq.NewRabbitMQWorkQueue("file", cfg)

	settingDao := repositories.NewSettingDao("configure", dbAuth, cfg)
	settingBs := server.NewSettingService(settingDao)

	uploadDao := repositories.NewUploadDao("file", dbAuth, cfg)
	uploadBs := server.NewUploadService(neo4jDb, minioClient, uploadDao, mq)

	g.Go(func() error {
		// 启动RabbitMQ消费者
		mq.ConsumeWorkQueue(func(combineFileList []byte) error {
			combineFileListStr := string(combineFileList)
			valueExtractType := strings.Split(combineFileListStr, "@")
			fileType := valueExtractType[1]
			// 消息格式 租户|fileName_1|fileName_2....&合并之后的文件名称@文件类型
			valueExtractFileName := strings.Split(valueExtractType[0], "&")
			if len(valueExtractFileName) <= 1 {
				fmt.Println("消息内容异常")
				return errors.New("消息内容异常")
			}
			fileFullName := valueExtractFileName[1]
			userAndfileList := strings.Split(valueExtractFileName[0], "|")
			if len(userAndfileList) <= 1 {
				fmt.Println("消息内容异常")
				return errors.New("消息内容异常")
			}
			userName := userAndfileList[0]
			fileList := make([]string, 0)
			for index, fileName := range userAndfileList {
				if index != 0 {
					fileList = append(fileList, fileName)
				}
			}
			return uploadBs.CombineFile(ctx, userName, fileList, fileFullName, fileType)
		})
		return nil
	})
	// http服务
	g.Go(func() error {
		httpListener, err := net.Listen("tcp", *httpAddr)
		if err != nil {
			fmt.Printf("http: net.Listen(tcp, %s) failed, err:%v\n", *httpAddr, err)
			return err
		}
		defer httpListener.Close()
		httpHandler := httpTransport.NewHTTPServer(settingBs, uploadBs)
		return http.Serve(httpListener, httpHandler)
	})
	if err := g.Wait(); err != nil {
		fmt.Printf("server exit with err:%v\n", err)
	}
}
