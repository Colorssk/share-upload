package server

import (
	"context"
	"file-upload/dataModels"
	"file-upload/util"
	"fmt"
	"strconv"
	"strings"
)
import "file-upload/repositories"

// 配置服务： 对服务做一些基础配置， 可以实现弹性控制： 控制当前系统
type ISettingSerive interface {
	GetSetting(context.Context, string) (interface{}, error)
	// 禁止上传的最大文件
	SetMaxSize(context.Context, float64) (bool, error)
	GetMaxSize(context.Context) (float64, error)
	// 可上传的文件类型
	SetValidFileType(context.Context, []string) (bool, error)
	GetValidFileType(context.Context) ([]string, error)
	// 分块阈值（高于阈值，分块，低于阈值一次性文件上传）
	SetThresHold(context.Context, float64) (bool, error)
	GetThreHold(context.Context) (float64, error)
	// 单独分块数据的大小，可调节
	SetBlockSize(context.Context, float64) (bool, error)
	GetBlockSize(context.Context) (float64, error)
	// 限制最大断点数目(也就是一次性同时可以有多少可以上传的数目)
	SetMaxBreaks(context.Context, int64) (bool, error)
	GetMaxBreaks(context.Context) (int64, error)
}
type SettingService struct {
	settingDao repositories.ISettingDao
}

func NewSettingService(settingDao repositories.ISettingDao) ISettingSerive {
	return &SettingService{settingDao: settingDao}
}

func (s *SettingService) GetSetting(ctx context.Context, key string) (interface{}, error) {
	settingRes, err := s.settingDao.GetSetting()
	if err != nil {
		return dataModels.Setting{}, err
	}
	if settingRes != nil {
		settingMap := util.StructToMap(*settingRes)
		fmt.Println("settingMap", settingMap)
		if val, ok := settingMap[key]; ok {
			return val, nil
		}
	}
	return nil, nil
}

// 设置可上传最大文件尺寸
func (s *SettingService) SetMaxSize(ctx context.Context, newValue float64) (bool, error) {
	return s.settingDao.EditSetting("maxSize", strconv.FormatFloat(newValue, 'E', -1, 64))
}
func (s *SettingService) GetMaxSize(ctx context.Context) (float64, error) {
	maxSizeValue, err := s.GetSetting(ctx, "maxSize")
	if err != nil {
		return 0, nil
	}
	if maxSizeValue == nil {
		return 0, nil
	}
	return maxSizeValue.(float64), nil
}

// 可上传文件类型
func (s *SettingService) SetValidFileType(ctx context.Context, newValue []string) (bool, error) {
	valueStr := strings.Join(newValue, ",")
	return s.settingDao.EditSetting("validFileType", valueStr)
}
func (s *SettingService) GetValidFileType(ctx context.Context) ([]string, error) {
	validFileTypeValue, err := s.GetSetting(ctx, "validFileType")
	if err != nil {
		return nil, nil
	}
	if validFileTypeValue == nil {
		return nil, nil
	}
	return strings.Split(validFileTypeValue.(string), ","), nil
}

// 分块阈值（高于阈值，分块，低于阈值一次性文件上传）
func (s *SettingService) SetThresHold(ctx context.Context, newValue float64) (bool, error) {
	return s.settingDao.EditSetting("thresHold", strconv.FormatFloat(newValue, 'E', -1, 64))
}
func (s *SettingService) GetThreHold(ctx context.Context) (float64, error) {
	thresHoldValue, err := s.GetSetting(ctx, "thresHold")
	if err != nil {
		return 0, nil
	}
	if thresHoldValue == nil {
		return 0, nil
	}
	return thresHoldValue.(float64), nil
}

// 单独分块数据的大小，可调节
func (s *SettingService) SetBlockSize(ctx context.Context, newValue float64) (bool, error) {
	return s.settingDao.EditSetting("blockSize", strconv.FormatFloat(newValue, 'E', -1, 64))
}
func (s *SettingService) GetBlockSize(ctx context.Context) (float64, error) {
	blockSizeValue, err := s.GetSetting(ctx, "blockSize")
	if err != nil {
		return 0, nil
	}
	if blockSizeValue == nil {
		return 0, nil
	}
	return blockSizeValue.(float64), nil
}

// 限制最大断点数目(也就是一次性同时可以有多少可以上传的数目)
func (s *SettingService) SetMaxBreaks(ctx context.Context, newValue int64) (bool, error) {
	return s.settingDao.EditSetting("maxBreaks", strconv.FormatInt(newValue, 10))
}
func (s *SettingService) GetMaxBreaks(ctx context.Context) (int64, error) {
	maxBreaksValue, err := s.GetSetting(ctx, "maxBreaks")
	if err != nil {
		return 0, nil
	}
	if maxBreaksValue == nil {
		return 0, nil
	}
	return maxBreaksValue.(int64), nil
}
