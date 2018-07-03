package cos

type Region string

// Constants of region definition
const (
	Shanghai  = Region("cn-east")
	Guangzhou = Region("cn-south")
	Tianjing  = Region("cn-north")
	Singapore = Region("sg")

	DefaultRegion = Guangzhou
)

// GetEndpoint returns endpoint of region
//func (r Region) GetEndpoint(bucket, appid string, secure bool) string {
//return r.GetDownloadEndpoint(bucket, appid, secure)
//}

func getProtocol(secure bool) string {
	protocol := "http"
	if secure {
		protocol = "https"
	}
	return protocol
}

//func (r Region) GetUploadEndpoint(secure bool) string {
//protocol := getProtocol(secure)
//return fmt.Sprintf("%s://%s.file.myqcloud.com", protocol, string(r))
//}

//func (r Region) GetDownloadEndpoint(bucket, appid string, secure bool) string {
//protocol := getProtocol(secure)
//return fmt.Sprintf("%s://%s-%s.%s.myqcloud.com", protocol, bucket, appid, string(r))
//}

//func (r Region) GetHost(bucket, appid string) string {
//return fmt.Sprintf("%s-%s.%s.myqcloud.com", bucket, appid, string(r))
//}
