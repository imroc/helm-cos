package cos

import (
	"bytes"
	"crypto/hmac"
	srand "crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const dictionary = "_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func MakeSha1(data string) string {
	t := sha1.New()
	t.Write([]byte(data))
	str := hex.EncodeToString(t.Sum(nil))
	return str
}

//CreateSignature creates signature for string
func CreateSignature(stringToSignature, accessKeySecret string) string {
	// Crypto by HMAC-SHA1
	hmacSha1 := hmac.New(sha1.New, []byte(accessKeySecret))
	hmacSha1.Write([]byte(stringToSignature))
	sign := hmacSha1.Sum(nil)

	hexSign := hex.EncodeToString(sign)

	return hexSign
}

func GetURIPath(u *url.URL) string {
	var uri string

	if len(u.Opaque) > 0 {
		uri = "/" + strings.Join(strings.Split(u.Opaque, "/")[3:], "/")
	} else {
		uri = u.EscapedPath()
	}

	if len(uri) == 0 {
		uri = "/"
	}

	return uri
}

//CreateRandomString create random string
func CreateRandomString() string {
	b := make([]byte, 32)
	l := len(dictionary)

	_, err := srand.Read(b)

	if err != nil {
		// fail back to insecure rand
		rand.Seed(time.Now().UnixNano())
		for i := range b {
			b[i] = dictionary[rand.Int()%l]
		}
	} else {
		for i, v := range b {
			b[i] = dictionary[v%byte(l)]
		}
	}

	return string(b)
}

// Encode encodes the values into ``URL encoded'' form
// ("acl&bar=baz&foo=quux") sorted by key.
func Encode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := url.QueryEscape(k)
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			if v != "" {
				buf.WriteString("=")
				buf.WriteString(url.QueryEscape(v))
			}
		}
	}
	return buf.String()
}

func GetGMTime() string {
	return time.Now().Format(http.TimeFormat)
}

// GetNowSec returns Unix time, the number of seconds elapsed since January 1, 1970 UTC.
// 获取当前时间，从UTC开始的秒数。
func GetNowSec() int64 {
	return time.Now().Unix()
}

// GetNowNanoSec returns t as a Unix time, the number of nanoseconds elapsed
// since January 1, 1970 UTC. The result is undefined if the Unix time
// in nanoseconds cannot be represented by an int64. Note that this
// means the result of calling UnixNano on the zero Time is undefined.
// 获取当前时间，从UTC开始的纳秒。
func GetNowNanoSec() int64 {
	return time.Now().UnixNano()
}

//

func randUint32() uint32 {
	return randUint32Slice(1)[0]
}

func randUint32Slice(c int) []uint32 {
	b := make([]byte, c*4)

	_, err := srand.Read(b)

	if err != nil {
		// fail back to insecure rand
		rand.Seed(time.Now().UnixNano())
		for i := range b {
			b[i] = byte(rand.Int())
		}
	}

	n := make([]uint32, c)

	for i := range n {
		n[i] = binary.BigEndian.Uint32(b[i*4 : i*4+4])
	}

	return n
}

func toByte(n uint32, st, ed byte) byte {
	return byte(n%uint32(ed-st+1) + uint32(st))
}

func toDigit(n uint32) byte {
	return toByte(n, '0', '9')
}

func toLowerLetter(n uint32) byte {
	return toByte(n, 'a', 'z')
}

func toUpperLetter(n uint32) byte {
	return toByte(n, 'A', 'Z')
}

type convFunc func(uint32) byte

var convFuncs = []convFunc{toDigit, toLowerLetter, toUpperLetter}

// tools for generating a random ECS instance password
// from 8 to 30 char MUST contain digit upper, case letter and upper case letter
// http://docs.aliyun.com/#/pub/ecs/open-api/instance&createinstance
func GenerateRandomECSPassword() string {

	// [8, 30]
	l := int(randUint32()%23 + 8)

	n := randUint32Slice(l)

	b := make([]byte, l)

	b[0] = toDigit(n[0])
	b[1] = toLowerLetter(n[1])
	b[2] = toUpperLetter(n[2])

	for i := 3; i < l; i++ {
		b[i] = convFuncs[n[i]%3](n[i])
	}

	s := make([]byte, l)
	perm := rand.Perm(l)
	for i, v := range perm {
		s[v] = b[i]
	}

	return string(s)

}
