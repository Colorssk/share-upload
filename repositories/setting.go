package repositories

import (
	"database/sql"
	"errors"
	"fmt"
	"gopkg.in/ini.v1"
)
import "file-upload/common"
import "file-upload/dataModels"

type ISettingDao interface {
	Conn() error
	// 默认杜宇configure表只有一条数据
	EditSetting(string, string) (bool, error)
	GetSetting() (*dataModels.Setting, error)
}

type SettingDao struct {
	settingTable string
	mysqlConn    *sql.DB
	cfg          *ini.File
}

func NewSettingDao(settingTable string, db *sql.DB, cfg *ini.File) ISettingDao {
	return &SettingDao{settingTable: settingTable, mysqlConn: db, cfg: cfg}
}

func (s *SettingDao) Conn() (err error) {
	if s.mysqlConn == nil {
		mysql, err := common.NewMysqlConn(s.cfg, "fileAssets")
		if err != nil {
			return err
		}
		s.mysqlConn = mysql
	}
	if s.settingTable == "" {
		s.settingTable = "configure"
	}
	return
}

func (s *SettingDao) EditSetting(editKey string, newValue string) (bool, error) {
	if err := s.Conn(); err != nil {
		return false, err
	}
	tx, err := s.mysqlConn.Begin()
	if err != nil {
		return false, err
	}
	sql := fmt.Sprintf("INSERT INTO %s (id, %s)\nVALUES (1, \"%s\")\nON DUPLICATE KEY UPDATE %s = \"%s\";", s.settingTable, editKey, newValue, editKey, newValue)
	fmt.Println("sql", sql)
	updateRes, err := tx.Exec(sql)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	_, err = updateRes.RowsAffected()
	if err != nil {
		tx.Rollback()
		return false, err
	}
	err = tx.Commit()
	if err != nil {
		return false, errors.New("更新失败")
	}
	return true, nil
}
func (s *SettingDao) GetSetting() (*dataModels.Setting, error) {
	if err := s.Conn(); err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("SELECT * FROM %s WHERE id=1", s.settingTable)
	row, err := s.mysqlConn.Query(sql)
	defer row.Close()
	if err != nil {
		return nil, err
	}
	results := common.GetResultRows(row)
	if len(results) == 0 {
		return nil, nil
	}
	settings := make([]*dataModels.Setting, 0)
	for _, v := range results {
		settingInfo := &dataModels.Setting{}
		common.DataToStructByTagSql(v, settingInfo)
		settings = append(settings, settingInfo)
	}
	return settings[0], nil
}
