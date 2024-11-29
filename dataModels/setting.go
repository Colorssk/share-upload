package dataModels

// 配置信息数据结构
type Setting struct {
	Id            int64   `json:"id" sql:"id" req:"id"`
	MaxSize       float64 `json:"maxSize" sql:"maxSize" req:"maxSize"`
	ValidFileType string  `json:"validFileType" sql:"validFileType" req:"validFileType"`
	ThresHold     float64 `json:"thresHold" sql:"thresHold" req:"thresHold"`
	BlockSize     float64 `json:"blockSize" sql:"blockSize" req:"blockSize"`
	MaxBreaks     int64   `json:"maxBreaks" sql:"maxBreaks" req:"maxBreaks"`
}
