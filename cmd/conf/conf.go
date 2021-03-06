package conf

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/imroc/helm-cos/pkg/cos"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"syscall"
	"golang.org/x/crypto/ssh/terminal"
)

type CosConfig struct {
	SecretId  string `json:"secret_id"`
	SecretKey string `json:"secret_key"`
}

type Config map[string]*CosConfig

var config Config

func getConfigFilename() string {
	basedir := os.Getenv("HELM_PLUGIN_DIR")
	if basedir == "" {
		panic("need env HELM_PLUGIN_DIR")
	}
	filename := basedir + string(os.PathSeparator) + "helm-cos.yaml"
	return filename
}

func getConfig() (Config, error) {
	if config != nil {
		return config, nil
	}
	config = make(Config)
	filename := getConfigFilename()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = yaml.Unmarshal(b, &config)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return config, nil
}

func InputCosConfig(bucket string) *CosConfig {
	println("Please login at first, enter your SecretId and SecretKey")
	var secretId, secretKey string
	print("SecretId:")
	fmt.Scanln(&secretId)
	print("SecretKey:")
	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	if err != nil {
		panic(err)
	}
	secretKey = string(bytePassword)

	if secretId == "" || secretKey == "" {
		println("Empty SecretId or SecretKey, please retry")
		return nil
	}

	cosConfig := &CosConfig{
		SecretId:  secretId,
		SecretKey: secretKey,
	}
	err = UpdateBucketConfig(bucket, cosConfig)
	if err != nil {
		panic(err)
	}
	return cosConfig
}

func ReadBucketConfig(bucket string) (*CosConfig, error) {
	c, err := getConfig()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cosConfig, ok := c[bucket]
	if !ok {
		return nil, nil
	}
	return cosConfig, nil
}

func GetBucketConfig(bucket string) (*CosConfig, error) {
	c, err := ReadBucketConfig(bucket)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if c != nil {
		return c, nil
	}
	c = InputCosConfig(bucket)
	return c, nil
}

func UpdateBucketConfig(bucket string, cosConfig *CosConfig) error {
	c, err := getConfig()
	if err != nil {
		return errors.WithStack(err)
	}
	c[bucket] = cosConfig
	b, err := yaml.Marshal(c)
	if err != nil {
		return errors.WithStack(err)
	}
	filename := getConfigFilename()
	err = ioutil.WriteFile(filename, b, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func GetCosClient(endpoint string) (*cos.Client, error) {
	cosConfig, err := GetBucketConfig(endpoint)
	if err != nil {
		return nil, err
	}
	if cosConfig == nil {
		return nil, errors.New("Empty SecretId or SecretKey")
	}
	client := &cos.Client{
		AccessKeyId:     cosConfig.SecretId,
		AccessKeySecret: cosConfig.SecretKey,
	}
	client.SetEndpoint(endpoint)
	return client, nil
}
