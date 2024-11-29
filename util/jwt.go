package util

import (
	"errors"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"time"
)

var jwtSecret = []byte("glenChain")

const JWT_CONTEXT_KEY = "colorssk"

type Token struct {
	Name    string
	DcId    int
	TokenId string
	jwt.StandardClaims
}

type RevokedToken struct {
	Token     string `json:"token"`
	RevokedAt int64  `json:"revoked_at"`
}

func CreateJwtToken(name string, dcId int) (string, error) {
	var token Token
	h, _ := time.ParseDuration("24h")
	tokenID := uuid.New().String()
	token.StandardClaims = jwt.StandardClaims{
		Audience:  "",                            // 受众群体
		ExpiresAt: time.Now().Add(30 * h).Unix(), // 到期时间
		Id:        tokenID,                       // 编号
		IssuedAt:  time.Now().Unix(),             // 签发时间
		Issuer:    name,                          // 签发人
		NotBefore: time.Now().Unix(),             // 生效时间
		Subject:   "login",                       // 主题
	}
	token.Name = name
	token.DcId = dcId
	token.TokenId = tokenID
	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, token)
	return tokenClaims.SignedString(jwtSecret)
}
func ParseToken(token string) (jwt.MapClaims, error) {
	jwtToken, err := jwt.ParseWithClaims(token, jwt.MapClaims{}, func(token *jwt.Token) (i interface{}, err error) {
		return jwtSecret, nil
	})
	if err != nil || jwtToken == nil {
		return nil, errors.New("token不合法")
	}
	claim, ok := jwtToken.Claims.(jwt.MapClaims)
	if ok && jwtToken.Valid {
		return claim, nil
	} else {
		return nil, errors.New("token不合法")
	}
}
