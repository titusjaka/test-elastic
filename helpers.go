package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

func getIdFromIpRange(ip ElasticIpRange) string {
	hasher := md5.New()
	hasher.Write([]byte(fmt.Sprintf("%s-%s", ip.StartIP, ip.EndIP)))
	return hex.EncodeToString(hasher.Sum(nil))
}
