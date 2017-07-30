package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"io/ioutil"
	"net/url"
	"os"

	"bytes"

	"github.com/apex/go-apex"
	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/upamune/amazing"
)

type message struct {
	QueryString QueryString `json:"queryStringParameters"`
}

type QueryString struct {
	ItemID string `json:"item_id"`
}

type service struct {
	client     *amazing.Amazing
	s3         *s3.S3
	bucketName string
}

type Response struct {
	Body string `json:"body"`
}

func main() {
	apex.HandleFunc(func(event json.RawMessage, context *apex.Context) (interface{}, error) {
		awsRegion := os.Getenv("AWS_REGION")
		awsBucket := os.Getenv("bucket")
		amazonAccess := os.Getenv("access")
		amazonSecret := os.Getenv("secret")
		amazonTag := os.Getenv("tag")
		amazonDomain := os.Getenv("domain")
		if amazonDomain == "" {
			amazonDomain = "JP"
		}

		client, err := amazing.NewAmazing(amazonDomain, amazonTag, amazonAccess, amazonSecret)
		if err != nil {
			log.Fatal(err.Error())
		}

		svc := s3.New(session.New(&aws.Config{Region: aws.String(awsRegion)}))

		service := &service{
			client:     client,
			s3:         svc,
			bucketName: awsBucket,
		}
		log.Info(string(event))
		var m message
		if err := json.Unmarshal(event, &m); err != nil {
			return nil, err
		}
		itemID := m.QueryString.ItemID

		if itemID == "" {
			return nil, fmt.Errorf("BadRequest: invalid item id: %s", itemID)
		}

		// キャッシュされていたらキャッシュを返す
		b, err := service.getItemFromCache(itemID)
		if err != nil {
			if err != ErrNotFoundFile {
				log.Warnf("failed get a cache: %s", err)
			}
		} else {
			return Response{Body: string(b)}, nil
		}

		params := url.Values{
			"IdType":        []string{"ASIN"},
			"ItemId":        []string{itemID},
			"Operation":     []string{"ItemLookup"},
			"ResponseGroup": []string{"Large"},
		}
		res, err := service.client.ItemLookup(params)
		if err != nil {
			return nil, fmt.Errorf("InternalError: failed to get item infomation: %v", err)
		}

		item, err := resToItem(res)
		if err != nil {
			return nil, fmt.Errorf("InternalError: failed to get item from response: %v", err)
		}

		// キャッシュする
		if err := service.saveItemToCache(item); err != nil {
			log.Warnf("failed to save a cache: %v", err)
		}

		b, err = json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("InternalError: failed to marshal json: %v", err)
		}
		log.Infof("item id(%s) json: %s", itemID, string(b))

		return Response{Body: string(b)}, nil
	})
}

type Item struct {
	ASIN         string
	Brand        string
	Creator      string
	Manufacturer string
	Publisher    string
	ReleaseDate  string
	Studio       string
	Title        string
	URL          string

	SmallImage  string
	MediumImage string
	LargeImage  string
}

var ErrNotFoundFile = errors.New("file not found")

func (s *service) getItemFromCache(itemID string) ([]byte, error) {
	filename := s.getFileName(itemID)

	obj, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(filename),
	})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()

	b, err := ioutil.ReadAll(obj.Body)
	if err != nil {
		return nil, ErrNotFoundFile
	}

	return b, nil
}

func (s *service) getFileName(itemID string) string {
	return fmt.Sprintf("amazon/%s", itemID)
}

func (s *service) saveItemToCache(item *Item) error {
	b, err := json.Marshal(item)
	if err != nil {
		return err
	}

	r := bytes.NewReader(b)
	_, err = s.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.getFileName(item.ASIN)),
		Body:   r,
	})

	return err
}

func resToItem(res *amazing.AmazonItemLookupResponse) (*Item, error) {
	items := res.AmazonItems.Items
	if len(items) == 0 {
		return nil, errors.New("empty amazon items")
	}

	aitem := items[0]

	item := &Item{
		ASIN:         aitem.ASIN,
		Brand:        aitem.ItemAttributes.Brand,
		Creator:      aitem.ItemAttributes.Creator,
		Manufacturer: aitem.ItemAttributes.Manufacturer,
		Publisher:    aitem.ItemAttributes.Publisher,
		ReleaseDate:  aitem.ItemAttributes.ReleaseDate,
		Studio:       aitem.ItemAttributes.Studio,
		Title:        aitem.ItemAttributes.Title,
		URL:          aitem.DetailPageURL,
		SmallImage:   aitem.SmallImage.URL,
		MediumImage:  aitem.MediumImage.URL,
		LargeImage:   aitem.LargeImage.URL,
	}

	return item, nil
}
