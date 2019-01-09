package main

import (
	"fmt"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"log"
	"sync/atomic"
	"time"
)

type ElasticClient struct {
	Connection *elastic.Client
}

func NewElasticClient(url string) (*ElasticClient, error) {
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		return nil, err
	}
	return &ElasticClient{
		Connection: client,
	}, nil
}

func (client *ElasticClient) Update(context context.Context, index string, bulkSize int, objects chan ElasticObject) error {
	var total uint64
	begin := time.Now()
	bulk := client.Connection.Bulk().Index(index).Type("_doc")
	for obj := range objects {
		current := atomic.AddUint64(&total, 1)
		duration := time.Since(begin).Seconds()
		seconds := int(duration)
		pps := int64(float64(current) / duration)
		fmt.Printf("%10d | %6d req/s | %02d:%02d\r", current, pps, seconds/60, seconds%60)

		select {
		case <-context.Done():
			log.Println("[DEBUG] Context is done")
			return context.Err()
		default:
			bulk.Add(elastic.NewBulkIndexRequest().Id(obj.ID).Doc(obj))
			if bulk.NumberOfActions() >= bulkSize {
				// Commit
				res, err := bulk.Do(context)
				if err != nil {
					log.Printf("[ERROR] %s\n", err.Error())
					return err
				}
				if res.Errors {
					log.Println("[ERROR] bulk commit failed")
					return errors.New("bulk commit failed")
				}
			}
		}
	}
	// Commit the final batch before exiting
	if bulk.NumberOfActions() > 0 {
		_, err := bulk.Do(context)
		if err != nil {
			return err
		}
	}
	// Final results
	dur := time.Since(begin).Seconds()
	sec := int(dur)
	pps := int64(float64(total) / dur)
	fmt.Printf("%10d | %6d req/s | %02d:%02d\n", total, pps, sec/60, sec%60)
	return nil
}
