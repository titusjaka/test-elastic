package main

type DataLine struct {
	StartIP           string `csv:"start-ip"`
	EndIP             string `csv:"end-ip"`
	Country           string `csv:"edge-two-letter-country"`
	Region            string `csv:"edge-region"`
	RegionCode        string `csv:"edge-region-code"`
	City              string `csv:"edge-city"`
	CityCode          string `csv:"edge-city-code"`
	ConnSpeed         string `csv:"edge-conn-speed"`
	ISP               string `csv:"isp-name"`
	MobileCarrier     string `csv:"mobile-carrier"`
	MobileCarrierCode string `csv:"mobile-carrier-code"`
}

type ElasticObject struct {
	ID                string         `json:"-"`
	IPAddress         ElasticIpRange `json:"ip_address"`
	Country           string         `json:"country"`
	Region            string         `json:"region"`
	RegionCode        string         `json:"region_code"`
	City              string         `json:"city"`
	CityCode          string         `json:"city_code"`
	ConnSpeed         string         `json:"conn_speed"`
	ISP               string         `json:"isp"`
	MobileCarrier     string         `json:"mobile_carrier"`
	MobileCarrierCode string         `json:"mobile_carrier_code"`
}

type ElasticIpRange struct {
	StartIP string `json:"gte"`
	EndIP   string `json:"lte"`
}

func csvLineToDataLine(csvLine []string) *DataLine {
	return &DataLine{
		StartIP:           csvLine[0],
		EndIP:             csvLine[1],
		Country:           csvLine[2],
		Region:            csvLine[3],
		RegionCode:        csvLine[4],
		City:              csvLine[5],
		CityCode:          csvLine[6],
		ConnSpeed:         csvLine[7],
		ISP:               csvLine[8],
		MobileCarrier:     csvLine[9],
		MobileCarrierCode: csvLine[10],
	}
}

func (dl *DataLine) toElasticObject() *ElasticObject {
	return &ElasticObject{
		IPAddress: ElasticIpRange{
			StartIP: dl.StartIP,
			EndIP:   dl.EndIP,
		},
		Country:           dl.Country,
		Region:            dl.Region,
		RegionCode:        dl.RegionCode,
		City:              dl.City,
		CityCode:          dl.CityCode,
		ConnSpeed:         dl.ConnSpeed,
		ISP:               dl.ISP,
		MobileCarrier:     dl.MobileCarrier,
		MobileCarrierCode: dl.MobileCarrierCode,
	}
}

func (eo *ElasticObject) toDataLine() *DataLine {
	return &DataLine{
		StartIP:           eo.IPAddress.StartIP,
		EndIP:             eo.IPAddress.EndIP,
		Country:           eo.Country,
		Region:            eo.Region,
		RegionCode:        eo.RegionCode,
		City:              eo.City,
		CityCode:          eo.CityCode,
		ConnSpeed:         eo.ConnSpeed,
		ISP:               eo.ISP,
		MobileCarrier:     eo.MobileCarrier,
		MobileCarrierCode: eo.MobileCarrierCode,
	}
}
