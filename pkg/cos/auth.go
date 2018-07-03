package cos

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

func (client *Client) signRequest(request *request) {

	//request.headers.Set("Date", GetGMTime())
	request.headers.Set("Host", client.GetHost(request.bucket))

	request.BuildAuth(client.AccessKeyId, client.AccessKeySecret)

	request.headers.Set("Authorization", request.Authorization)
}

func (req *request) buildSignKey(accessKeySecret string) {
	now := GetNowSec()
	expire := req.expire.Unix()
	req.SignTime = fmt.Sprintf("%d;%d", now, expire)
	//req.SignTime = "1480932292;1481012292"
	req.KeyTime = req.SignTime
	req.SignKey = CreateSignature(req.KeyTime, accessKeySecret)
}

func canonicalHeaders(h http.Header) string {
	i, a, lowerCase := 0, make([]string, len(h)), make(map[string][]string)

	for k, v := range h {
		lowerCase[strings.ToLower(k)] = v
	}

	var keys []string
	for k := range lowerCase {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := lowerCase[k]
		for j, w := range v {
			v[j] = url.QueryEscape(strings.Trim(w, " "))
		}
		sort.Strings(v)
		a[i] = strings.ToLower(k) + "=" + strings.Join(v, ",")
		i++
	}
	return strings.Join(a, "&")
}

func canonicalQueryString(u *url.URL) string {
	keyValues := make(map[string]string, len(u.Query()))
	keys := make([]string, len(u.Query()))

	key_i := 0
	for k, vs := range u.Query() {
		k = strings.ToLower(k)
		k = url.QueryEscape(k)
		k = strings.ToLower(k)

		a := make([]string, len(vs))
		for idx, v := range vs {
			v = url.QueryEscape(v)
			a[idx] = fmt.Sprintf("%s=%s", k, v)
		}

		keyValues[k] = strings.Join(a, "&")
		keys[key_i] = k
		key_i++
	}

	sort.Strings(keys)

	query := make([]string, len(keys))
	for idx, key := range keys {
		query[idx] = keyValues[key]
	}

	query_str := strings.Join(query, "&")

	return strings.Replace(query_str, "+", "%20", -1)
}

func (req *request) buildFormatString() {
	FormatMethod := strings.ToLower(req.method)

	u, err := req.url()
	if err != nil {
		panic(err)
	}

	FormatURI := GetURIPath(u)
	if DEBUG {
		log.Printf("uri:%s, u:%#v", FormatURI, u)
	}
	//FormatParameters := req.params.Encode()

	//FormatParameters := u.RawQuery
	FormatParameters := canonicalQueryString(u)
	if DEBUG {
		log.Printf("uri:%s, u:%#v", FormatURI, u)
	}

	var headers = []string{}
	headerQuery := url.Values{}
	for k, v := range req.headers {
		lowerCaseKey := strings.ToLower(k)
		headers = append(headers, lowerCaseKey)

		for _, item := range v {
			headerQuery.Add(lowerCaseKey, item)
		}
	}

	sort.Strings(headers)
	req.HeaderList = strings.Join(headers, ";")

	FormatHeaders := fmt.Sprintf("%s", headerQuery.Encode())
	//FormatHeaders = url.QueryEscape(FormatHeaders)
	//FormatHeaders = canonicalHeaders(req.headers)
	//FormatHeaders = strings.ToLower(FormatHeaders)

	if DEBUG {
		log.Printf("formatpara:%v, formatheader:%s,%s", FormatParameters, FormatHeaders, strings.ToLower(FormatHeaders))
	}
	req.FormatString = strings.Join([]string{
		FormatMethod,
		FormatURI,
		FormatParameters,
		FormatHeaders,
	}, "\n") + "\n"
}

func (req *request) buildStringToSign() {
	shaStr := MakeSha1(req.FormatString)
	if DEBUG {
		log.Println(shaStr)
	}
	req.StringToSign = strings.Join([]string{
		SignAlgorithm,
		req.SignTime,
		shaStr,
	}, "\n") + "\n"
}

func (req *request) buildSignature() {

	req.Signature = CreateSignature(req.StringToSign, req.SignKey)
}

func (req *request) buildParamList() {
	var paraKeys = []string{}
	for k, _ := range req.params {
		lowerCaseKey := strings.ToLower(k)
		paraKeys = append(paraKeys, lowerCaseKey)
	}
	sort.Strings(paraKeys)

	req.UrlParamList = strings.Join(paraKeys, ";")
}

func (client *Client) signURLRequest(request *request) {

	request.BuildAuth(client.AccessKeyId, client.AccessKeySecret)

	//request.params.Set(URLSignPara, url.QueryEscape(request.Authorization))
	// request.params.Set(URLSignPara, request.Authorization)
}

const (
	QSignAlgorithm = "q-sign-algorithm"
	QAK            = "q-ak"
	QSignTime      = "q-sign-time"
	QKeyTime       = "q-key-time"
	QHeaderList    = "q-header-list"
	QUrlParamList  = "q-url-param-list"
	QSign          = "q-signature"

	SignAlgorithm = "sha1"

	URLSignPara = "sign"
)

func (req *request) buildAuthorization(secretId string) {
	req.params.Set(QSignAlgorithm, SignAlgorithm)
	req.params.Set(QAK, secretId)
	req.params.Set(QSignTime, req.SignTime)
	req.params.Set(QKeyTime, req.KeyTime)
	req.params.Set(QHeaderList, req.HeaderList)
	req.params.Set(QUrlParamList, req.UrlParamList)
	req.params.Set(QSign, req.Signature)

	// req.Authorization = strings.Join([]string{
	// 	QSignAlgorithm + "=" + SignAlgorithm,
	// 	QAK + "=" + secretId,
	// 	QSignTime + "=" + req.SignTime,
	// 	QKeyTime + "=" + req.KeyTime,
	// 	QHeaderList + "=" + req.HeaderList,
	// 	QUrlParamList + "=" + req.UrlParamList,
	// 	QSign + "=" + req.Signature,
	// }, "&")

}

func (req *request) BuildAuth(secretId, secretKey string) {
	req.buildSignKey(secretKey)
	req.buildFormatString()
	req.buildStringToSign()
	req.buildSignature()
	req.buildParamList()
	req.buildAuthorization(secretId)
	if DEBUG {
		log.Printf("%#v", req)
	}

}

func (req *request) url() (*url.URL, error) {
	u, err := url.Parse(req.baseurl)
	if err != nil {
		return nil, fmt.Errorf("bad COS endpoint URL %q: %v", req.baseurl, err)
	}
	u.RawQuery = req.params.Encode()
	//u.RawQuery = strings.ToLower(u.RawQuery)
	//u.RawQuery = strings.Replace(req.params.Encode(), "+", "%20", -1)
	u.RawQuery = strings.Replace(u.RawQuery, "uploads=", "uploads", -1)
	u.RawQuery = strings.Replace(u.RawQuery, "delete=", "delete", -1)
	u.Path = req.path

	return u, nil
}
