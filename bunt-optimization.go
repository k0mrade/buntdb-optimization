package main

// URL
// http://10.0.0.253:9002/join?nbiIP=10.0.0.241&wlan=58&reason=Un-Auth-Captive&loc=4f6666696365&mac=6c:aa:b3:1b:fa:50&uip=ENC1bf058f5336ccb5e32d7046a0c3f5ea9&url=http%3A%2F%2Fmazda.ua%2F&zoneName=ON+AIR+Office&client_mac=ENCa09ad3bc1b0383ef61e611b452b69a78d4efdec5e514fdde&sip=194.242.99.241&proxy=0&ssid=9002&wlanName=9002&dn=
// KEY
// 1470113127851157834 f0:3e:90:2e:4f:d0
// VALUE
// JSON SessionValue

import (
	"fmt"
	"log"
	"time"

	"github.com/jinzhu/now"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
)

func main() {
	start := time.Now()
	dbbunt, err := buntdb.Open("buntnew.db")
	if err != nil {
		log.Fatal(err)
	}
	defer dbbunt.Close()
	fmt.Printf("Reading Bunt database --> %v\n", time.Since(start))

	start = time.Now()
	dbbunt.CreateIndex("AdActionTime", "*", buntdb.IndexJSON("AdActionTime"))
	fmt.Printf("JSON index AdActionTime creation time --> %v\n", time.Since(start))
	idx, _ := dbbunt.Indexes()
	fmt.Printf("Indexes list --> %v\n", idx)

	var sk []string
	un := make(map[string]string)
	start = time.Now()
	err = dbbunt.View(func(tx *buntdb.Tx) error {
		tx.Ascend("AdActionTime", func(key, val string) bool {
			mac := gjson.Get(val, "SZUrl.ClientMac")
			// t := gjson.Get(val, "AdActionTime")
			un[mac.String()] = val
			sk = append(sk, val)
			// fmt.Printf("Staion %v connects at %v\n", mac.String(), t.String()) //len(sk))
			return true
		})
		return nil
	})
	fmt.Printf("Reading %v rows from BuntDB was --> %v\nUnique --> %v\n", len(sk), time.Since(start), len(un))

	var sk1 []string
	start = time.Now()
	err = dbbunt.View(func(tx *buntdb.Tx) error {
		tx.AscendRange("AdActionTime", "1470936865210540500", "1470999986453908700", func(key, val string) bool {
			fmt.Println(key)
			sk1 = append(sk1, key)
			return true
		})
		fmt.Println(tx)
		return nil
	})
	fmt.Printf("Range reading %v rows from BuntDB was --> %v\n", len(sk1), time.Since(start))

	// start = time.Now()
	// d := time.Unix(0, 1470790800000000000)
	// sc, usc, err := DayCounter(d, dbbunt)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Printf("Stations --> %v\nUnique stations --> %v\n", sc, usc)
	// fmt.Printf("DayCounter retured value for --> %v\n", time.Since(start))
}

func counter(hr [][]int64, idx string, db *buntdb.DB) (<-chan int, <-chan int, <-chan int) {
	var lsk []string
	un := make(map[string]string)
	c1 := make(chan int)
	c2 := make(chan int)
	c3 := make(chan int)
	for i, h := range hr {
		go func(h []int64, i int) {
			// fmt.Println(time.Unix(0, h[0]), time.Unix(0, h[1]))
			start := time.Now()
			err := db.View(func(tx *buntdb.Tx) error {
				tx.AscendRange(idx, string(h[0]), string(h[1]), func(key, val string) bool {
					fmt.Println(key)
					return true
					// date := strings.Split(key, " ")[0]
					// if date > string(h[0]) && date < string(h[1]) {
					// 	lsk = append(lsk, key)
					// 	un[key] = val
					// 	return true
					// }
					// // un[key[27:78]] = val
					// // 	unAP1[key[9:26]] = val // AP MAC
					// // lsk = append(lsk, key[135:])
					// return false
				})
				return nil
			})
			if err != nil {
				fmt.Println("Error")
			}
			fmt.Printf("During %v:00–%v:00\t was %v actions, %v unique mac from index %v was --> %v\n", i, (i + 1), len(lsk), len(un), idx, time.Since(start))
			c1 <- i
			c2 <- len(un)
			c3 <- len(lsk)
		}(h, i)
	}
	return c1, c2, c3
}

// DayCounter ...
func DayCounter(date time.Time, db *buntdb.DB) ([24]int, [24]int, error) {
	var err error
	var sk, usk [24]int
	hr := HoursInDay(date)
	c1, c2, c3 := counter(hr, "AdActionTime", db)
	for i := 0; i < 24; i++ {
		hour := <-c1
		sk[hour] = <-c2
		usk[hour] = <-c3
	}
	return sk, usk, err
}

// HoursInDay returns the array of two value – beginnig and end of hour.
func HoursInDay(t time.Time) [][]int64 {
	var hsbe [][]int64
	hour := int64(3600000000000)
	bd := now.New(t).BeginningOfDay()
	bdn := bd.UnixNano()
	edn := bdn + hour - 1 //now.New(bd).EndOfHour().UnixNano()
	for i := 0; i < 24; i++ {
		hsbe = append(hsbe, []int64{bdn, edn})
		bdn = bdn + hour
		edn = edn + hour
	}
	return hsbe
}

// DaysInMonth returns the array of two value – beginnig and end of day.
func DaysInMonth(t time.Time) [][]int64 {
	var dsbe [][]int64
	day := int64(86400000000000)
	bm := now.New(t).BeginningOfMonth()
	em := now.New(bm).EndOfMonth()
	bdn := bm.UnixNano()
	edn := bdn + day - int64(1)
	for i := 1; i <= em.Day(); i++ {
		dsbe = append(dsbe, []int64{bdn, edn})
		bdn = bdn + day
		edn = edn + day
	}
	return dsbe
}

// MonthInYear returns the array of two value – beginnig and end of month.
func MonthInYear(t time.Time, loc *time.Location) [][]int64 {
	var msbe [][]int64
	year := now.New(t).Year()
	for i := 1; i < 13; i++ {
		fdm := time.Date(year, time.Month(i), 01, 0, 0, 0, 0, loc)
		bm := now.New(fdm).BeginningOfMonth()
		em := now.New(bm).EndOfMonth()
		bmn := bm.UnixNano()
		emn := em.UnixNano()
		msbe = append(msbe, []int64{bmn, emn})
	}
	return msbe
}

// SessionValue ...
type SessionValue struct {
	AdShowTime   int64
	AdActionTime int64
	AdStopTime   int64
	UserAgent    UserAgent
	SZURL        SZURL
	AD           SessionMarker
}

// SessionMarker ...
type SessionMarker struct {
	Name   string `json:"Name"`
	Type   int    `json:"Type"`
	Choice int    `json:"Choice,omitempty"`
}

// SZURL generated by SZ
type SZURL struct {
	NbIP       string `schema:"nbiIP"`      // Northbound IP
	ClientMac  string `schema:"client_mac"` // Client MAC
	DomainName string `schema:"dn"`         // Domain name
	Loaction   string `schema:"loc"`        // Location
	MAC        string `schema:"mac"`        // Acccess point mac address
	Proxy      string `schema:"proxy"`
	Reason     string `schema:"reason"`
	Sip        string `schema:"sip"`
	SSID       string `schema:"ssid"`
	StartURL   string `schema:"startUrl"`
	UIP        string `schema:"uip"` // Station IP
	URL        string `schema:"url"`
	VLAN       string `schema:"vlan"`
	WLAN       string `schema:"wlan"`
	ZoneID     string `schema:"zoneId"`
	ZoneName   string `schema:"zoneName"`
}

// UserAgent ...
type UserAgent struct {
	OS           string
	Platform     string
	Engine       Engine
	Browser      Browser
	Localization string
}

// Engine ...
type Engine struct {
	Name    string
	Version string
}

// Browser ...
type Browser struct {
	Name    string
	Version string
}
