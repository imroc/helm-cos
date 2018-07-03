package cos

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Multi represents an unfinished multipart upload.
//
// Multipart uploads allow sending big objects in smaller chunks.
// After all parts have been sent, the upload must be explicitly
// completed by calling Complete with the list of parts.

type Multi struct {
	Bucket   *Bucket
	Key      string
	UploadID string `xml:"UploadId"`
}

// That's the default. Here just for testing.
var listMultiMax = 1000

type listMultiResp struct {
	NextKeyMarker      string
	NextUploadIdMarker string
	IsTruncated        bool
	Upload             []Multi
	CommonPrefixes     []string `xml:"CommonPrefixes>Prefix"`
}

// ListMulti returns the list of unfinished multipart uploads in b.
//
// The prefix parameter limits the response to keys that begin with the
// specified prefix. You can use prefixes to separate a bucket into different
// groupings of keys (to get the feeling of folders, for example).
//
// The delim parameter causes the response to group all of the keys that
// share a common prefix up to the next delimiter in a single entry within
// the CommonPrefixes field. You can use delimiters to separate a bucket
// into different groupings of keys, similar to how folders would work.
//
func (b *Bucket) ListMulti(prefix, delim string) (multis []*Multi, prefixes []string, err error) {
	params := make(url.Values)
	params.Set("uploads", "")
	//params.Set("max-uploads", strconv.FormatInt(int64(listMultiMax), 10))
	params.Set("prefix", prefix)
	params.Set("delimiter", delim)

	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "GET",
			bucket: b.Name,
			params: params,
			expire: time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		var resp listMultiResp
		err := b.Client.query(req, &resp)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		for i := range resp.Upload {
			multi := &resp.Upload[i]
			multi.Bucket = b
			multis = append(multis, multi)
		}
		prefixes = append(prefixes, resp.CommonPrefixes...)
		if !resp.IsTruncated {
			return multis, prefixes, nil
		}
		params.Set("key-marker", resp.NextKeyMarker)
		params.Set("upload-id-marker", resp.NextUploadIdMarker)
		attempt = attempts.Start() // Last request worked.
	}
	panic("unreachable")
}

func hasCode(err error, code string) bool {
	e, ok := err.(*Error)
	return ok && e.Code == code
}

// Multi returns a multipart upload handler for the provided key
// inside b. If a multipart upload exists for key, it is returned,
// otherwise a new multipart upload is initiated with contType and perm.
func (b *Bucket) Multi(key, contType string, perm ACL, options Options) (*Multi, error) {
	multis, _, err := b.ListMulti(key, "")
	if err != nil && !hasCode(err, "NoSuchUpload") {
		return nil, err
	}
	for _, m := range multis {
		if m.Key == key {
			return m, nil
		}
	}
	return b.InitMulti(key, contType, perm, options)
}

// InitMulti initializes a new multipart upload at the provided
// key inside b and returns a value for manipulating it.
//
//
//
func (b *Bucket) InitMulti(key string, contType string, perm ACL, options Options) (*Multi, error) {
	headers := make(http.Header)
	headers.Set("Content-Length", "0")
	headers.Set("Content-Type", contType)
	//headers.Set("x-cos-acl", string(perm))

	options.addHeaders(headers)
	params := make(url.Values)
	params.Set("uploads", "")
	req := &request{
		method:  "POST",
		bucket:  b.Name,
		path:    key,
		headers: headers,
		params:  params,
		expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
	}
	var err error
	var resp struct {
		UploadId string `xml:"UploadId"`
	}
	for attempt := attempts.Start(); attempt.Next(); {
		err = b.Client.query(req, &resp)
		if !shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &Multi{Bucket: b, Key: key, UploadID: resp.UploadId}, nil
}

// CopyObjectResult is the output from a Copy request
type CopyObjectResult struct {
	ETag         string
	LastModified string
}

func (m *Multi) PutPartCopy(n int, options CopyOptions, source string) (*CopyObjectResult, Part, error) {
	return m.PutPartCopyWithContentLength(n, options, source, -1)
}

//
//
func (m *Multi) PutPartCopyWithContentLength(n int, options CopyOptions, source string, contentLength int64) (*CopyObjectResult, Part, error) {
	// TODO source format a /BUCKET/PATH/TO/OBJECT
	// TODO not a good design. API could be changed to PutPartCopyWithinBucket(..., path) and PutPartCopyFromBucket(bucket, path)

	headers := make(http.Header)
	headers.Set("x-cos-copy-source", source)

	options.addHeaders(headers)
	params := make(url.Values)
	params.Set("uploadId", m.UploadID)
	params.Set("partNumber", strconv.FormatInt(int64(n), 10))

	if contentLength < 0 {
		sourceBucket := m.Bucket.Client.Bucket(strings.TrimRight(strings.Split(source, "/")[1], "/"))
		//log.Println("source: ", source)
		//log.Println("sourceBucket: ", sourceBucket.Name)
		//log.Println("HEAD: ", strings.strings.SplitAfterN(source, "/", 3)[2])
		// TODO SplitAfterN can be use in bucket name
		sourceMeta, err := sourceBucket.Head(strings.SplitAfterN(source, "/", 3)[2], nil)
		if err != nil {
			return nil, Part{}, err
		}
		contentLength = sourceMeta.ContentLength
	}

	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method:  "PUT",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		resp := &CopyObjectResult{}
		err := m.Bucket.Client.query(req, resp)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, Part{}, err
		}
		if resp.ETag == "" {
			return nil, Part{}, errors.New("part upload succeeded with no ETag")
		}
		return resp, Part{n, resp.ETag, contentLength}, nil
	}
	panic("unreachable")
}

// PutPart sends part n of the multipart upload, reading all the content from r.
// Each part, except for the last one, must be at least 5MB in size.
//
//
//
func (m *Multi) PutPart(n int, r io.ReadSeeker) (Part, error) {
	partSize, _, hexsha1, err := seekerInfo(r)
	if err != nil {
		return Part{}, err
	}
	return m.putPart(n, r, partSize, hexsha1, 0)
}

func (m *Multi) PutPartWithTimeout(n int, r io.ReadSeeker, timeout time.Duration) (Part, error) {
	partSize, _, hexsha1, err := seekerInfo(r)
	if err != nil {
		return Part{}, err
	}
	return m.putPart(n, r, partSize, hexsha1, timeout)
}

func (m *Multi) putPart(n int, r io.ReadSeeker, partSize int64, hexsha1 string, timeout time.Duration) (Part, error) {
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(partSize, 10))
	headers.Set("x-cos-content-sha1", hexsha1)

	params := make(url.Values)
	params.Set("uploadId", m.UploadID)
	params.Set("partNumber", strconv.FormatInt(int64(n), 10))

	for attempt := attempts.Start(); attempt.Next(); {
		_, err := r.Seek(0, 0)
		if err != nil {
			return Part{}, err
		}
		req := &request{
			method:  "PUT",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: r,
			timeout: timeout,
			expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err = m.Bucket.Client.prepare(req, false)
		if err != nil {
			return Part{}, err
		}
		resp, err := m.Bucket.Client.run(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return Part{}, err
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			return Part{}, errors.New("part upload succeeded with no ETag")
		}
		return Part{n, etag, partSize}, nil
	}
	panic("unreachable")
}

func seekerInfo(r io.ReadSeeker) (size int64, sha1b64 string, sha1hex string, err error) {
	_, err = r.Seek(0, 0)
	if err != nil {
		return 0, "", "", err
	}
	digest := sha1.New()
	size, err = io.Copy(digest, r)
	if err != nil {
		return 0, "", "", err
	}
	sum := digest.Sum(nil)
	sha1hex = hex.EncodeToString(sum)
	sha1b64 = base64.StdEncoding.EncodeToString(sum)
	return size, sha1b64, sha1hex, nil
}

func seekerInfoMD5(r io.ReadSeeker) (size int64, md5hex string, md5b64 string, err error) {
	_, err = r.Seek(0, 0)
	if err != nil {
		return 0, "", "", err
	}
	digest := md5.New()
	size, err = io.Copy(digest, r)
	if err != nil {
		return 0, "", "", err
	}
	sum := digest.Sum(nil)
	md5hex = hex.EncodeToString(sum)
	md5b64 = base64.StdEncoding.EncodeToString(sum)
	return size, md5hex, md5b64, nil
}

type Part struct {
	N    int `xml:"PartNumber"`
	ETag string
	Size int64
}

type partSlice []Part

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].N < s[j].N }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type listPartsResp struct {
	NextPartNumberMarker string
	IsTruncated          bool
	Part                 []Part
}

// That's the default. Here just for testing.
var listPartsMax = 1000

// ListParts for backcompatability. See the documentation for ListPartsFull
func (m *Multi) ListParts() ([]Part, error) {
	return m.ListPartsFull(0, listPartsMax)
}

// ListPartsFull returns the list of previously uploaded parts in m,
// ordered by part number (Only parts with higher part numbers than
// partNumberMarker will be listed). Only up to maxParts parts will be
// returned.
//
func (m *Multi) ListPartsFull(partNumberMarker int, maxParts int) ([]Part, error) {
	if maxParts > listPartsMax {
		maxParts = listPartsMax
	}

	params := make(url.Values)
	params.Set("uploadId", m.UploadID)
	params.Set("max-parts", strconv.FormatInt(int64(maxParts), 10))
	params.Set("part-number-marker", strconv.FormatInt(int64(partNumberMarker), 10))

	var parts partSlice
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "GET",
			bucket: m.Bucket.Name,
			path:   m.Key,
			params: params,
			expire: time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		var resp listPartsResp
		err := m.Bucket.Client.query(req, &resp)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, err
		}
		parts = append(parts, resp.Part...)
		if !resp.IsTruncated {
			sort.Sort(parts)
			return parts, nil
		}
		params.Set("part-number-marker", resp.NextPartNumberMarker)
		attempt = attempts.Start() // Last request worked.
	}
	panic("unreachable")
}

type ReaderAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

type completeUpload struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   completeParts `xml:"Part"`
}

type completePart struct {
	PartNumber int
	ETag       string
}

type completeParts []completePart

func (p completeParts) Len() int           { return len(p) }
func (p completeParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p completeParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Complete assembles the given previously uploaded parts into the
// final object. This operation may take several minutes.
//
func (m *Multi) Complete(parts []Part) error {
	params := make(url.Values)
	params.Set("uploadId", m.UploadID)

	c := completeUpload{}
	for _, p := range parts {
		c.Parts = append(c.Parts, completePart{p.N, p.ETag})
	}
	sort.Sort(c.Parts)
	data, err := xml.Marshal(&c)
	if err != nil {
		return err
	}

	if DEBUG {
		log.Printf("parts:%v, data:%v\n", c, data)
	}

	payload := bytes.NewReader(data)
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))

	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method:  "POST",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: payload,
			expire:  time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err := m.Bucket.Client.query(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		return err
	}
	panic("unreachable")
}

// Abort deletes an unifinished multipart upload and any previously
// uploaded parts for it.
//
// After a multipart upload is aborted, no additional parts can be
// uploaded using it. However, if any part uploads are currently in
// progress, those part uploads might or might not succeed. As a result,
// it might be necessary to abort a given multipart upload multiple
// times in order to completely free all storage consumed by all parts.
//
// NOTE: If the described scenario happens to you, please report back to
// the goamz authors with details. In the future such retrying should be
// handled internally, but it's not clear what happens precisely (Is an
// error returned? Is the issue completely undetectable?).
//
//

func (m *Multi) Abort() error {
	params := make(url.Values)
	params.Set("uploadId", m.UploadID)

	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "DELETE",
			bucket: m.Bucket.Name,
			path:   m.Key,
			params: params,
			expire: time.Now().Add(DefaultSignExpireTime * time.Second),
		}
		err := m.Bucket.Client.query(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		return err
	}
	panic("unreachable")
}
