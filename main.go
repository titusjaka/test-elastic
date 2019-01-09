package main

import (
	"flag"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"log"
)

var (
	url      = flag.String("url", "http://localhost:9200", "Elasticsearch URL")
	index    = flag.String("index", "geoip_index", "Elasticsearch index name")
	typ      = flag.String("type", "_doc", "Elasticsearch type name")
	filename = flag.String("filename", "", "Path to SCV with GEO-info")
	bulkSize = flag.Int("bulksize", 10000, "Number of documents to collect before committing")
)

type GeoElastic struct {
	ID          string      `json:"id"`
	IP          IPRange     `json:"ip_addr"`
	Information ElasticInfo `json:"info"`
}

type IPRange struct {
	Start string `json:"gte"`
	End   string `json:"lte"`
}

type ElasticInfo struct {
	CountryISO      string `json:"two-letter-country"`
	RegionISO       string `json:"region"`
	RegionCode      string `json:"region-code"`
	City            string `json:"city"`
	CityCode        string `json:"city-code"`
	ConnectionSpeed string `json:"conn-speed"`
	MobileIPS       string `json:"mobile-carrier"`
	MobileIPSCode   string `json:"mobile-carrier-code"`
	ProxyType       string `json:"proxy-type"`
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	if *url == "" {
		log.Fatal("missing url parameter")
	}
	if *index == "" {
		log.Fatal("missing index parameter")
	}
	if *typ == "" {
		log.Fatal("missing type parameter")
	}
	if *filename == "" {
		log.Fatal("missing PATH to CSV-file")
	}
	if *bulkSize <= 0 {
		log.Fatal("bulk-size must be a positive number")
	}

	client, err := NewElasticClient(*url)
	if err != nil {
		log.Fatal(err)
	}

	csvChan := make(chan DataLine)
	elasticChan := make(chan ElasticObject)

	gr, ctx := errgroup.WithContext(context.Background())

	gr.Go(
		func() error {
			return client.Update(ctx, *index, *bulkSize, elasticChan)
		},
	)

	gr.Go(
		func() error {
			defer log.Println("[DEBUG] CSV channel is closed")
			defer close(csvChan)
			return readDataFromCSV(*filename, ctx, csvChan)
		},
	)

	gr.Go(
		func() error {
			defer log.Println("[DEBUG] Elastic channel is closed")
			defer close(elasticChan)
			for line := range csvChan {
				eo := line.toElasticObject()
				id := getIdFromIpRange(eo.IPAddress)
				eo.ID = id
				elasticChan <- *eo
			}
			return nil
		},
	)

	err = gr.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
