package repositories

import (
	"database/sql"
	"errors"
	"file-upload/common"
	"file-upload/dataModels"
	"fmt"
	"gopkg.in/ini.v1"
)

type IUploadDao interface {
	Conn() error
	AddFile(dataModels.FileInfo) (bool, error)
	GetFileInfo(string) (*dataModels.FileInfo, error)
}

type UploadDao struct {
	uploadTable string
	mysqlConn   *sql.DB
	cfg         *ini.File
}

func NewUploadDao(uploadTable string, db *sql.DB, cfg *ini.File) IUploadDao {
	return &UploadDao{uploadTable: uploadTable, mysqlConn: db, cfg: cfg}
}

func (u *UploadDao) Conn() (err error) {
	if u.mysqlConn == nil {
		mysql, err := common.NewMysqlConn(u.cfg, "fileAssets")
		if err != nil {
			return err
		}
		u.mysqlConn = mysql
	}
	if u.uploadTable == "" {
		u.uploadTable = "file"
	}
	return
}

func (u *UploadDao) AddFile(newFile dataModels.FileInfo) (bool, error) {
	if err := u.Conn(); err != nil {
		return false, err
	}
	tx, err := u.mysqlConn.Begin()
	if err != nil {
		return false, err
	}
	sql := fmt.Sprintf("INSERT INTO %s (fileHashInTenant, path)\nVALUES (\"%s\", \"%s\")\nON DUPLICATE KEY UPDATE path=\"%s\";", u.uploadTable, newFile.FileHashInTenant, newFile.Path, newFile.Path)
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
func (u *UploadDao) GetFileInfo(fileHashInTenant string) (*dataModels.FileInfo, error) {
	if err := u.Conn(); err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("SELECT * FROM %s WHERE fileHashInTenant=\"%s\"", u.uploadTable, fileHashInTenant)
	row, err := u.mysqlConn.Query(sql)
	defer row.Close()
	if err != nil {
		return nil, err
	}
	results := common.GetResultRows(row)
	if len(results) == 0 {
		return nil, nil
	}
	fils := make([]*dataModels.FileInfo, 0)
	for _, v := range results {
		fileInfo := &dataModels.FileInfo{}
		common.DataToStructByTagSql(v, fileInfo)
		fils = append(fils, fileInfo)
	}
	return fils[0], nil
}
