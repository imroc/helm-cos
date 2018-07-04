package conf

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/imroc/helm-cos/pkg/cos"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
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
	fmt.Println("config filename:", filename)
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
	err = yaml.Unmarshal(b, config)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return config, nil
}

func inputCosConfig(bucket string) *CosConfig {
	fmt.Println("Please login at first, enter your SecretId and SecretKey")
	var secretId, secretKey string
	for {
		fmt.Print("SecretId:")
		fmt.Scanln(&secretId)
		fmt.Print("SecretKey:")
		fmt.Scanln(&secretKey)
		if secretId == "" || secretKey == "" {
			fmt.Println("Empty SecretId or SecretKey, please retry")
			continue
		}

		cosConfig := &CosConfig{
			SecretId:  secretId,
			SecretKey: secretKey,
		}
		err := UpdateBucketConfig(bucket, cosConfig)
		if err != nil {
			panic(err)
		}
		return cosConfig
	}

}

func GetBucketConfig(bucket string) (*CosConfig, error) {
	c, err := getConfig()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fmt.Printf("get config: %+v\n",c)
	cosConfig, ok := c[bucket]
	if !ok {
		cosConfig = inputCosConfig(bucket)
	}
	return cosConfig, nil
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
	client := &cos.Client{
		AccessKeyId:     cosConfig.SecretId,
		AccessKeySecret: cosConfig.SecretKey,
	}
	client.SetEndpoint(endpoint)
	return client, nil
}
