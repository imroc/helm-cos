package cos

import (
	"bytes"
	"crypto/md5"
	//"crypto/sha1"
	"encoding/base64"
	//"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultContentType = "application/octet-stream"
const DefaultSignExpireTime = 86400

var DEBUG bool

// The Client type encapsulates operations with an COS region.
type Client struct {
	AppId           string
	AccessKeyId     string
	AccessKeySecret string
	Region          Region
	Secure          bool
	ConnectTimeout  time.Duration

	host     string
	endpoint string
	Debug    bool
}

// The Bucket type encapsulates operations with an bucket.
type Bucket struct {
	*Client
	Name string
}

func (client *Client) GetEndpoint(bucket string) string {
	if client.endpoint != "" {
		return client.endpoint
	}
	protocol := getProtocol(client.Secure)
	return fmt.Sprintf("%s://%s-%s.%s.myqcloud.com", protocol, bucket, client.AppId, string(client.Region))
}

func (client *Client) GetHost(bucket string) string {
	if bucket == "" && client.host != "" {
		return client.host
	}
	return fmt.Sprintf("%s-%s.%s.myqcloud.com", bucket, client.AppId, string(client.Region))
}

func (b *Bucket) Head(path string, headers http.Header) (*http.Response, error) {

	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method:  "HEAD",
			bucket:  b.Name,
			path:    path,
			headers: headers,
			expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err := b.Client.prepare(req, false)
		if err != nil {
			return nil, err
		}

		resp, err := b.Client.run(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, err
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return resp, err
	}
	return nil, fmt.Errorf("Currently Unreachable")
}

// Get retrieves an object from an bucket.
//
// You can read doc at https://www.qcloud.com/document/product/436/7753
func (b *Bucket) Get(path string) (data []byte, err error) {
	body, err := b.GetReader(path)
	if err != nil {
		return nil, err
	}
	data, err = ioutil.ReadAll(body)
	body.Close()
	return data, err
}

// GetReader retrieves an object from an bucket,
// returning the body of the HTTP response.
// It is the caller's responsibility to call Close on rc when
// finished reading.
func (b *Bucket) GetReader(path string) (rc io.ReadCloser, err error) {
	resp, err := b.GetResponse(path)
	if resp != nil {
		return resp.Body, err
	}
	return nil, err
}

// GetResponse retrieves an object from an bucket,
// returning the HTTP response.
// It is the caller's responsibility to call Close on rc when
// finished reading
func (b *Bucket) GetResponse(path string) (resp *http.Response, err error) {
	return b.GetResponseWithHeaders(path, make(http.Header))
}

// GetResponseWithHeaders retrieves an object from an bucket
// Accepts custom headers to be sent as the second parameter
// returning the body of the HTTP response.
// It is the caller's responsibility to call Close on rc when
// finished reading
func (b *Bucket) GetResponseWithHeaders(path string, headers http.Header) (resp *http.Response, err error) {
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			bucket:  b.Name,
			path:    path,
			headers: headers,
			expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err = b.Client.prepare(req, false)
		if err != nil {
			return nil, err
		}

		resp, err := b.Client.run(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	panic("unreachable")
}

// Options struct
//
type Options struct {
	Meta               map[string][]string
	ContentEncoding    string
	CacheControl       string
	COSContentSHA1     string
	ContentDisposition string

	//Expires string
	//Expect  string
}

// addHeaders adds o's specified fields to headers
func (o Options) addHeaders(headers http.Header) {
	if len(o.ContentEncoding) != 0 {
		headers.Set("Content-Encoding", o.ContentEncoding)
	}
	if len(o.CacheControl) != 0 {
		headers.Set("Cache-Control", o.CacheControl)
	}
	if len(o.COSContentSHA1) != 0 {
		headers.Set("x-cos-content-sha1", o.COSContentSHA1)
	}
	if len(o.ContentDisposition) != 0 {
		headers.Set("Content-Disposition", o.ContentDisposition)
	}

	for k, v := range o.Meta {
		for _, mv := range v {
			headers.Add("x-cos-meta-"+k, mv)
		}
	}
}

type CopyOptions struct {
	Headers           http.Header
	CopySourceOptions string
	MetadataDirective string
	//ContentType       string
}

// addHeaders adds o's specified fields to headers
func (o CopyOptions) addHeaders(headers http.Header) {
	if len(o.MetadataDirective) != 0 {
		headers.Set("x-cos-metadata-directive", o.MetadataDirective)
	}
	if len(o.CopySourceOptions) != 0 {
		headers.Set("x-cos-copy-source-range", o.CopySourceOptions)
	}
	if o.Headers != nil {
		for k, v := range o.Headers {
			newSlice := make([]string, len(v))
			copy(newSlice, v)
			headers[k] = newSlice
		}
	}
}

type ACL string

const (
	Private    = ACL("private")
	PublicRead = ACL("public-read")
)

func (b *Bucket) Put(path string, data []byte, contType string, perm ACL, options Options) error {
	body := bytes.NewBuffer(data)
	return b.PutReader(path, body, int64(len(data)), contType, perm, options)
}

// PutReader inserts an object into the bucket by consuming data
// from r until EOF.
func (b *Bucket) PutReader(path string, r io.Reader, length int64, contType string, perm ACL, options Options) error {
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	headers.Set("Content-Type", contType)
	//headers.Set("x-cos-acl", string(perm))

	options.addHeaders(headers)
	req := &request{
		method:  "PUT",
		bucket:  b.Name,
		path:    path,
		headers: headers,
		payload: r,
		expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
	}
	return b.Client.query(req, nil)
}

// PutCopy puts a copy of an object given by the key path into bucket b using b.Path as the target key
//
//
func (b *Bucket) PutCopy(path string, perm ACL, options CopyOptions, source string) (*CopyObjectResult, error) {
	headers := make(http.Header)

	//headers.Set("x-cos-acl", string(perm))
	headers.Set("x-cos-copy-source", source)

	options.addHeaders(headers)
	req := &request{
		method:  "PUT",
		bucket:  b.Name,
		path:    path,
		headers: headers,
		timeout: 5 * time.Minute,
		expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
	}
	resp := &CopyObjectResult{}
	err := b.Client.query(req, resp)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

type Delete struct {
	Quiet   bool     `xml:"Quiet,omitempty"`
	Objects []Object `xml:"Object"`
}

type Object struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

func makeXMLBuffer(doc []byte) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteString(xml.Header)
	buf.Write(doc)
	return buf
}

// DelMulti removes up to 1000 objects from the bucket.
//
//
func (b *Bucket) DelMulti(objects Delete) error {
	doc, err := xml.Marshal(objects)
	if err != nil {
		return err
	}

	buf := makeXMLBuffer(doc)
	//digest := sha1.New()
	digest := md5.New()
	size, err := digest.Write(buf.Bytes())
	if err != nil {
		return err
	}

	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(int64(size), 10))
	//headers.Set("x-cos-content-sha1", hex.EncodeToString(digest.Sum(nil)))
	//headers.Set("Content-MD5", hex.EncodeToString(digest.Sum(nil)))
	headers.Set("Content-MD5", base64.StdEncoding.EncodeToString(digest.Sum(nil)))
	headers.Set("Content-Type", "text/xml")

	req := &request{
		path:    "/",
		method:  "POST",
		params:  url.Values{"delete": {""}},
		bucket:  b.Name,
		headers: headers,
		payload: buf,
		expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
	}

	return b.Client.query(req, nil)
}

func (b *Bucket) Del(path string) error {
	req := &request{
		method: "DELETE",
		bucket: b.Name,
		path:   path,
		expire: time.Now().Add(DefaultSignExpireTime * time.Second),
	}
	return b.Client.query(req, nil)
}

func (b *Bucket) AddBucket(path string) error {
	req := &request{
		method: "PUT",
		bucket: b.Name,
		path:   path,
		expire: time.Now().Add(DefaultSignExpireTime * time.Second),
	}
	return b.Client.query(req, nil)
}

// The ListResp type holds the results of a List bucket operation.
type ListResp struct {
	Name    string
	Prefix  string
	Marker  string
	MaxKeys int
	// IsTruncated is true if the results have been truncated because
	// there are more keys and prefixes than can fit in MaxKeys.
	// N.B. this is the opposite sense to that documented (incorrectly) in
	// http://goo.gl/YjQTc
	IsTruncated    bool
	Contents       []Key
	CommonPrefixes []string `xml:">Prefix"`
	// if IsTruncated is true, pass NextMarker as marker argument to List()
	// to get the next set of keys
	NextMarker string
}

// The Key type represents an item stored in an bucket.
type Key struct {
	Key          string
	LastModified string
	Size         int64
	// ETag gives the hex-encoded MD5 sum of the contents,
	// surrounded with double-quotes.
	ETag         string
	Owner        Owner
	StorageClass string
}

// The Owner type represents the owner of the object in an bucket.
type Owner struct {
	ID string
}

func (b *Bucket) List(prefix, delim, marker string, max int) (result *ListResp, err error) {
	params := make(url.Values)
	params.Set("prefix", prefix)
	params.Set("delimiter", delim)
	params.Set("marker", marker)
	if max != 0 {
		params.Set("max-keys", strconv.FormatInt(int64(max), 10))
	}
	result = &ListResp{}
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			bucket: b.Name,
			params: params,
			expire: time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err = b.Client.query(req, result)
		if !shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	// if NextMarker is not returned, it should be set to the name of last key,
	// so let's do it so that each caller doesn't have to
	if result.IsTruncated && result.NextMarker == "" {
		n := len(result.Contents)
		if n > 0 {
			result.NextMarker = result.Contents[n-1].Key
		}
	}
	return result, nil
}

func (b *Bucket) Path(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return "/" + b.Name + path
}

// SignedURL returns a signed URL that allows anyone holding the URL
// to retrieve the object at path. The signature is valid until expires.
func (b *Bucket) SignedURL(path string, expires time.Time) string {
	return b.SignedURLWithArgs(path, expires, nil, nil)
}

// SignedURLWithArgs returns a signed URL that allows anyone holding the URL
// to retrieve the object at path. The signature is valid until expires.
func (b *Bucket) SignedURLWithArgs(path string, expires time.Time, params url.Values, headers http.Header) string {
	return b.SignedURLWithMethod("GET", path, expires, params, headers)
}

// SignedURLWithMethod returns a signed URL that allows anyone holding the URL
// to either retrieve the object at path or make a HEAD request against it. The signature is valid until expires.
func (b *Bucket) SignedURLWithMethod(method, path string, expires time.Time, params url.Values, headers http.Header) string {
	var uv = url.Values{}

	if params != nil {
		uv = params
	}

	req := &request{
		method:  method,
		bucket:  b.Name,
		path:    path,
		params:  uv,
		headers: headers,
		expire:  expires,
	}
	err := b.Client.prepare(req, true)
	if err != nil {
		panic(err)
	}
	u, err := req.url()
	if err != nil {
		panic(err)
	}

	return u.String()
}

type TimeoutError interface {
	error
	Timeout() bool // Is the error a timeout?
}

// Error represents an error in an operation with COS.
type Error struct {
	StatusCode int    // HTTP status code (200, 403, ...)
	Code       string // COS error code ("UnsupportedOperation", ...)
	Message    string // The human-oriented error message
	Resource   string
	RequestId  string
	TraceId    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("qcloud API Error: RequestId: %s Status Code: %d Code: %s Message: %s", e.RequestId, e.StatusCode, e.Code, e.Message)
}

func (client *Client) buildError(r *http.Response) error {
	if client.Debug {
		log.Printf("got error (status code %v)", r.StatusCode)
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("\tread error: %v", err)
		} else {
			log.Printf("\tdata:\n%s\n\n", data)
		}
		r.Body = ioutil.NopCloser(bytes.NewBuffer(data))
	}

	err := Error{}
	// TODO return error if Unmarshal fails?
	xml.NewDecoder(r.Body).Decode(&err)
	r.Body.Close()
	err.StatusCode = r.StatusCode
	if err.Message == "" {
		err.Message = r.Status
	}
	if client.Debug {
		log.Printf("err: %#v\n", err)
	}
	return &err
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(TimeoutError)
	if ok {
		return true
	}

	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		return true
	}
	switch e := err.(type) {
	case *net.DNSError:
		return true
	case *net.OpError:
		switch e.Op {
		case "read", "write":
			return true
		}
	case *url.Error:
		// url.Error can be returned either by net/url if a URL cannot be
		// parsed, or by net/http if the response is closed before the headers
		// are received or parsed correctly. In that later case, e.Op is set to
		// the HTTP method name with the first letter uppercased. We don't want
		// to retry on POST operations, since those are not idempotent, all the
		// other ones should be safe to retry.
		switch e.Op {
		case "Get", "Put", "Delete", "Head":
			return shouldRetry(e.Err)
		default:
			return false
		}
	case *Error:
		switch e.Code {
		case "InternalError", "NoSuchUpload", "NoSuchBucket", "PathConflict":
			return true
		}
	}
	return false
}

var attempts = AttemptStrategy{
	Min:   5,
	Total: 5 * time.Second,
	Delay: 1000 * time.Millisecond,
}

// NewCOSClient creates a new COS.

func NewCOSClient(region Region, AppId string, accessKeyId string, accessKeySecret string, secure bool, debug bool) *Client {
	DEBUG = debug
	return &Client{
		AppId:           AppId,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Region:          region,
		Debug:           debug,
		Secure:          secure,
	}
}

// query prepares and runs the req request.
// If resp is not nil, the XML data contained in the response
// body will be unmarshalled on it.
func (client *Client) query(req *request, resp interface{}) error {
	err := client.prepare(req, false)
	if err != nil {
		return err
	}
	r, err := client.run(req, resp)
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
	return err
}

// partiallyEscapedPath partially escapes the COS path allowing for all COS REST API calls.
//
// Some commands including:
//      GET Bucket acl              https://www.qcloud.com/document/api/436/7733
// require the first character after the bucket name in the path to be a literal '?' and
// not the escaped hex representation '%3F'.
func partiallyEscapedPath(path string) string {
	pathEscapedAndSplit := strings.Split((&url.URL{Path: path}).String(), "/")
	//log.Printf("pathEscapedAndSplit:%#v\n", pathEscapedAndSplit)
	if len(pathEscapedAndSplit) >= 2 {
		if len(pathEscapedAndSplit[1]) >= 3 {
			// Check for the one "?" that should not be escaped.
			if pathEscapedAndSplit[1][0:3] == "%3F" {
				pathEscapedAndSplit[1] = "?" + pathEscapedAndSplit[1][3:]
			}
		}
	}
	return strings.Replace(strings.Join(pathEscapedAndSplit, "/"), "+", "%2B", -1)
}

// doHttpRequest sends hreq and returns the http response from the server.
// If resp is not nil, the XML data contained in the response
// body will be unmarshalled on it.
func (client *Client) doHttpRequest(c *http.Client, hreq *http.Request, resp interface{}) (*http.Response, error) {

	if true {
		log.Printf("%s %s ...\n", hreq.Method, hreq.URL.String())
	}
	hresp, err := c.Do(hreq)
	if err != nil {
		return nil, err
	}
	if client.Debug {
		log.Printf("%s %s %d\n", hreq.Method, hreq.URL.String(), hresp.StatusCode)
		log.Printf("http url:%#v", hreq.URL)
		contentType := hresp.Header.Get("Content-Type")
		if contentType == "application/xml" || contentType == "text/xml" {
			dump, _ := httputil.DumpResponse(hresp, true)
			log.Printf("%s\n", dump)
		} else {
			log.Printf("Response Content-Type: %s\n", contentType)
		}
	}
	if hresp.StatusCode != 200 && hresp.StatusCode != 204 && hresp.StatusCode != 206 {
		return nil, client.buildError(hresp)
	}
	if resp != nil {
		err = xml.NewDecoder(hresp.Body).Decode(resp)
		hresp.Body.Close()

		if client.Debug {
			log.Printf("cos> decoded xml into %#v", resp)
		}

	}
	return hresp, err
}

// Prepares an *http.Request for doHttpRequest
func (client *Client) setupHttpRequest(req *request) (*http.Request, error) {
	// Copy so that signing the http request will not mutate it

	u, err := req.url()
	if err != nil {
		return nil, err
	}
	u.Opaque = fmt.Sprintf("//%s%s", u.Host, partiallyEscapedPath(u.Path))
	if client.Debug {
		log.Printf("setupHttpRequest path:%v, opaque:%v", u.Path, u.Opaque)
	}
	u.Host = client.GetHost(req.bucket)

	hreq := http.Request{
		URL:        u,
		Method:     req.method,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Close:      true,
		Header:     req.headers,
		Form:       req.params,
	}

	contentLength := req.headers.Get("Content-Length")

	if contentLength != "" {
		hreq.ContentLength, _ = strconv.ParseInt(contentLength, 10, 64)
		req.headers.Del("Content-Length")
	}

	if req.payload != nil {
		hreq.Body = ioutil.NopCloser(req.payload)
	}

	return &hreq, nil
}

func (client *Client) run(req *request, resp interface{}) (*http.Response, error) {
	if client.Debug {
		log.Printf("Running COS request: %#v", req)
	}

	hreq, err := client.setupHttpRequest(req)
	if err != nil {
		return nil, err
	}
	hreq.Host = hreq.URL.Host
	//hreq.RequestURI = "/"

	if client.Debug {
		log.Printf("http request:%#v, url:%#v", hreq, hreq.URL)
	}

	c := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (c net.Conn, err error) {
				if client.ConnectTimeout > 0 {
					c, err = net.DialTimeout(netw, addr, client.ConnectTimeout)
				} else {
					c, err = net.Dial(netw, addr)
				}
				if err != nil {
					return
				}
				return
			},
			Proxy: http.ProxyFromEnvironment,
		},
		Timeout: req.timeout,
	}

	return client.doHttpRequest(c, hreq, resp)
}

func copyHeader(header http.Header) (newHeader http.Header) {
	newHeader = make(http.Header)
	for k, v := range header {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newHeader[k] = newSlice
	}
	return
}

func (client *Client) prepare(req *request, isURLSign bool) error {
	// Copy so they can be mutated without affecting on retries.
	headers := copyHeader(req.headers)
	params := make(url.Values)

	for k, v := range req.params {
		params[k] = v
	}

	req.params = params
	req.headers = headers

	if !req.prepared {
		req.prepared = true
		if req.method == "" {
			req.method = "GET"
		}

		if !strings.HasPrefix(req.path, "/") {
			req.path = "/" + req.path
		}

		err := client.setBaseURL(req)
		if err != nil {
			return err
		}
	}

	if !isURLSign {
		client.signRequest(req)
	} else {
		client.signURLRequest(req)
	}

	return nil
}

func (client *Client) setBaseURL(req *request) error {

	if client.endpoint == "" {
		req.baseurl = client.GetEndpoint(req.bucket)
	} else {
		req.baseurl = client.endpoint
	}

	return nil
}

// SetDebug sets debug mode to log the request/response message
func (client *Client) SetDebug(debug bool) {
	client.Debug = debug
}

// Bucket returns a Bucket with the given name.
func (client *Client) Bucket(name string) *Bucket {
	name = strings.ToLower(name)
	return &Bucket{
		Client: client,
		Name:   name,
	}
}

// override default endpoint
func (client *Client) SetEndpoint(endpoint string) {
	// TODO check endpoint
	if endpoint != "" {
		link := fmt.Sprintf("%s://%s", getProtocol(client.Secure), endpoint)
		u, err := url.Parse(link)
		if err != nil {
			panic(err)
		}
		client.host = u.Host
		client.endpoint = link
	}
}

type request struct {
	method   string
	bucket   string
	path     string
	params   url.Values
	headers  http.Header
	baseurl  string
	prepared bool
	payload  io.Reader
	timeout  time.Duration
	expire   time.Time //time point

	SignKey       string
	FormatString  string
	StringToSign  string
	Authorization string

	SignTime     string
	KeyTime      string
	HeaderList   string
	UrlParamList string
	Signature    string
}

func Test() {
	req := &request{
		method:  "GET",
		path:    "/testfile",
		baseurl: "http://testbucket-125000000.cn-north.myqcloud.com",
	}

	headers := make(http.Header)
	headers.Add("Host", "testbucket-125000000.cn-north.myqcloud.com")
	headers.Add("Range", "bytes=0-3")
	req.headers = headers

	secretId := "QmFzZTY0IGlzIGEgZ2VuZXJp"
	secretKey := "AKIDZfbOA78asKUYBcXFrJD0a1ICvR98JM"
	req.BuildAuth(secretId, secretKey)
	fmt.Println(req.Authorization)
}
