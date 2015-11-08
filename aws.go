package main
import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"path/filepath"
	"os"
	"io"
	"crypto/aes"
	"crypto/cipher"
	"log"
	"path"
)

func main() {


	args := os.Args

	if args = os.Args; ! (len(args)==3 && !filepath.IsAbs(args[1])) {
		log.Fatal("usage: relativePath")
	}

	p := args[1]

	t := args[2]

	c := config{}
	if e := c.ReadOrNew(p); e != nil {
		log.Fatal(e)
	}

	sess := session.New(&aws.Config{Region: aws.String("eu-west-1")})
	s3serv := s3.New(sess)

	if t == "push" {
		push(s3serv, c, p)
	}else if t == "pull" {
		pull(s3serv, c, p)
	}



}

func pull(s3serv *s3.S3, c config, p string) {
	if list, e := s3serv.ListObjects(&s3.ListObjectsInput{Bucket: &c.Bucket}); e == nil {
		for _, l := range (list.Contents) {
			log.Println(*l.Key)
			if obj, e := s3serv.GetObject(&s3.GetObjectInput{Bucket: &c.Bucket, Key: l.Key}); e == nil {
				os.MkdirAll(path.Dir(p+ *l.Key), 0700)
				if f, e := os.OpenFile(p+ *l.Key, os.O_CREATE |os.O_TRUNC|os.O_WRONLY, 0666); e== nil {
					defer f.Close()
					defer obj.Body.Close()
					counter := &CountReader{input:obj.Body, co:c}
					counter.Init()
					io.Copy(f, counter)
					log.Println("written", *l.Key)
				}                else {
					log.Println(e)
				}

			}else {
				log.Println(e)
			}
		}
	}else {
		log.Fatal(e)
	}

}


func push(s3serv *s3.S3, c config, p string) {
	createBucket(c.Bucket, s3serv)
	files := make(chan string)

	go walkFiles(p, files)
	recvUpload(s3serv, c, files)

	fmt.Println("done")
}

type CountReader struct {
	co    config
	ci    cipher.StreamReader
	input io.Reader
	seeker io.Seeker
	count int64
	ov    int64
}

func (c *CountReader) Seek(offset int64, whence int) (int64, error) {

	c.Init()
	return c.seeker.Seek(offset, whence)
}

func (c *CountReader) Init() {
	c.count=0
	c.ov=0
	bl, _ := aes.NewCipher(c.co.Key)

	s := cipher.NewCTR(bl, make([]byte, bl.BlockSize()))
	c.ci = cipher.StreamReader{s, c.input}

}

func (c *CountReader) Read(p []byte) (n int, err error) {

	n, err = c.ci.Read(p)
	c.count += int64(n)
	if c.ov += int64(n); c.ov > 100000 {
		fmt.Print(".")
		c.ov = c.ov -100000
	}
	return n, err
}

func recvUpload(s3serv *s3.S3, c config, files chan string) {

	for {
		path, closed := <-files

		if !closed {
			break
		}

		if f, err := os.Open(path); err == nil {
			counter := &CountReader{input:f,seeker:f, co:c}
			counter.Init()
			fmt.Print("uploading:", path)
			if _, err := s3serv.PutObject(&s3.PutObjectInput{Bucket: &c.Bucket, Key: &path, Body: counter});
			err == nil {
				fmt.Println("done", counter.count)

			}else {
				fmt.Println("error uploading file:", err)
			}

		}else {
			fmt.Printf("error opening file:", err)
		}
	}
}

func createBucket(bucket string, s3serv *s3.S3) {


	if resp, err := s3serv.ListBuckets(nil); err == nil {
		for _, b := range (resp.Buckets) {
			if *b.Name == bucket {
				return
			}
		}

		if resp, err := s3serv.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket)}); err==nil {
		} else {
			fmt.Println(err, resp)
		}

	}
}



func walkFiles(path string, files chan string) {
	defer close(files)
	fmt.Println(path)
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files <- path
		}
		return nil
	})

}