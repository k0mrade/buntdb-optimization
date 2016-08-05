package main

// URL
// http://10.0.0.253:9002/join?nbiIP=10.0.0.241&wlan=58&reason=Un-Auth-Captive&loc=4f6666696365&mac=6c:aa:b3:1b:fa:50&uip=ENC1bf058f5336ccb5e32d7046a0c3f5ea9&url=http%3A%2F%2Fmazda.ua%2F&zoneName=ON+AIR+Office&client_mac=ENCa09ad3bc1b0383ef61e611b452b69a78d4efdec5e514fdde&sip=194.242.99.241&proxy=0&ssid=9002&wlanName=9002&dn=
// KEY
// AD000008 f0:3e:90:2e:4f:d0 ENCf9a8611a1c7139d045d8e5e451203ce2adc5e47c3dbe2157 ENC5da4cec7b32712378088a788f46e881d 1470113127851157834 1470113140954825778
// VALUE

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/k0mrade/buntdb"
)

func main() {
	start := time.Now()
	db, err := bolt.Open("_bolt.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Printf("Reading Bolt database %v\n", time.Since(start))
	start = time.Now()
	dbbunt, err := buntdb.Open("_bunt100K.db")
	if err != nil {
		log.Fatal(err)
	}
	defer dbbunt.Close()
	fmt.Printf("Reading Bunt database %v\n", time.Since(start))
	start = time.Now()
	// dbbunt.CreateIndex("AD000007", "AD000007 *", buntdb.IndexString)
	// dbbunt.CreateIndex("AD000008", "AD000008 *", buntdb.IndexString)
	// dbbunt.CreateIndex("ENCf9a8611a1c7139d045d8e5e451203ce2adc5e47c3dbe2157", "* * ENCf9a8611a1c7139d045d8e5e451203ce2adc5e47c3dbe2157 * * *", buntdb.IndexString)
	// dbbunt.CreateIndex("ENC225a6673d276676d6a9456482504256da4401487d286bf4f", "* * ENC225a6673d276676d6a9456482504256da4401487d286bf4f * * *", buntdb.IndexString)
	dbbunt.CreateIndex("AdActionTime", "*", buntdb.IndexJSON("AdActionTime"))
	fmt.Printf("JSON index creation time --> %v\n", time.Since(start))
	idx, _ := dbbunt.Indexes()
	fmt.Printf("Indexes list --> %v\n", idx)
	// start := time.Now()
	// // dbbunt.CreateIndex("AdActionTime", "*", buntdb.IndexJSON("AdActionTime"))
	// // fmt.Printf("Время создания индекса AdActionTime --> %v\n", time.Since(start))
	// // start = time.Now()
	// dbbunt.CreateIndex("SZUrl.ClientMac", "*", buntdb.IndexJSON("SZUrl.ClientMac"))
	// fmt.Printf("Время создания индекса SZUrl.ClientMac --> %v\n", time.Since(start))
	// start = time.Now()
	// dbbunt.CreateIndex("ApMac", "*", buntdb.IndexJSON("MAC"))
	// fmt.Printf("Время создания индекса ApMac --> %v\n", time.Since(start))
	// bsl, _ := ByteSessionList(db)
	// kv := make(map[string]string)
	// start = time.Now()
	// err = dbbunt.View(func(tx *buntdb.Tx) error {
	// 	tx.Ascend("", func(key, val string) bool {
	// 		kv[key] = val
	// 		// fmt.Printf("%s\n", key)
	// 		return true
	// 	})
	// 	return nil
	// })
	// fmt.Printf("Время время чтения из базы --> %v\n", time.Since(start))
	// kv1 := make(map[string]string)
	// start = time.Now()
	// err = dbbunt.View(func(tx *buntdb.Tx) error {
	// 	tx.Ascend("AD000008", func(key, val string) bool {
	// 		kv1[key] = val
	// 		// fmt.Printf("%s\n", key)
	// 		return true
	// 	})
	// 	return nil
	// })
	// fmt.Printf("Время время чтения индекса AD000008 из базы --> %v\n", time.Since(start))
	// kv2 := make(map[string]string)
	// start = time.Now()
	// err = dbbunt.View(func(tx *buntdb.Tx) error {
	// 	tx.Ascend("AD000007", func(key, val string) bool {
	// 		kv2[key] = val
	// 		// fmt.Printf("%s\n", key)
	// 		return true
	// 	})
	// 	return nil
	// })
	// fmt.Printf("Время время чтения индекса AD000007 из базы --> %v\n", time.Since(start))
	// t := time.Date(2016, time.January, 01, 0, 0, 0, 0, time.UTC)
	// bm := now.New(t).BeginningOfMonth().UnixNano()
	// ed := now.New(t).EndOfDay().UnixNano()
	// em := now.New(t).EndOfMonth().UnixNano()
	// eq := now.New(t).EndOfQuarter().UnixNano()
	// fmt.Println(bm, ed, em)
	kv3 := make(map[string]string)
	counter := 0
	start = time.Now()
	err = dbbunt.View(func(tx *buntdb.Tx) error {
		tx.Ascend("", func(key, val string) bool {
			// tx.AscendRange("AdActionTime", string(bm), string(ed), func(key, val string) bool {
			kv3[key] = val
			sv, _ := DecodeSessionValue([]byte(val))
			if sv.SZUrl.ClientMac != "" {
				fmt.Printf("%s\n", val)
				counter++
			}
			return true
		})
		return nil
	})
	fmt.Println(counter)
	fmt.Printf("Время время чтения диапазона индекса AD000007 из базы --> %v\n", time.Since(start))
	// kv4 := make(map[string]string)
	// start = time.Now()
	// err = dbbunt.View(func(tx *buntdb.Tx) error {
	// 	tx.AscendRange("", string(bm), string(eq), func(key, val string) bool {
	// 		fmt.Printf("%s\n", key)
	// 		if key[:7] == "AD000007" {
	// 			kv4[key] = val
	// 			fmt.Printf("%s\n", key)
	// 		}
	// 		return true
	// 	})
	// 	return nil
	// })
	// fmt.Printf("Время время чтения диапазона AD000007 из базы --> %v\n", time.Since(start))
}

func generateDB(bss ByteSessions, multiplier int, db *buntdb.DB) error {
	var err error
	var randTime int64
	var Avt float64
	start := time.Now()
	for i, bs := range bss {
		err = db.Update(func(tx *buntdb.Tx) error {
			for i := 0; i <= multiplier; i++ {
				randTime = random(1451606400000000000+10000000000000*int64(i), 1483228799999999999)
				// fmt.Println(time.Unix(0, randTime).Format("02-01-2006 15:04:05.000000000"))
				sk := DecodeSessionKey(bs.Key)
				sk.AdActionTime = randTime
				sks := SessionKeyToString(sk)
				_, _, err := tx.Set(sks, string(bs.Value), nil)
				if err != nil {
					return err
				}
			}
			return err
		})
		if err != nil {
			return err
		}
		avt := time.Since(start).Seconds() / float64(multiplier)
		Avt = Avt + avt
		fmt.Printf("Итерация %v\nSET oer sec %v\nКлюч %v\n", i, avt, string(bs.Key))
	}
	fmt.Printf("Время генерации базы BuntDB --> %v\nСреднее время SET --> %v\n", time.Since(start), Avt/25591)
	return nil
}

func random(min, max int64) int64 {
	rand.Seed(time.Now().Unix())
	return rand.Int63n(max-min) + min
}

// SessionKeyToString ecodes SessionKey to a string session key
func SessionKeyToString(sk SessionKey) string {
	st := strconv.FormatInt(sk.AdShowTime, 10)
	at := strconv.FormatInt(sk.AdActionTime, 10)
	enc := sk.AdID + " " + sk.ApMAC + " " + sk.StationMAC + " " + sk.StationIP + " " + st + " " + at
	return enc
}

// SessionKey ...
type SessionKey struct {
	AdID         string
	ApMAC        string
	StationMAC   string
	StationIP    string
	AdShowTime   int64
	AdActionTime int64
}

// SessionValue ...
type SessionValue struct {
	AdShowTime   int64
	AdActionTime int64
	AdStopTime   int64
	UserAgent    UserAgent
	SZUrl        SZURL
}

// ByteSession ...
type ByteSession struct {
	Key   []byte
	Value []byte
}

// ByteSessions ...
type ByteSessions []ByteSession

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

// EncodeSessionKey ecodes SessionKey to the bytes session key
// Bolt implementation
func EncodeSessionKey(sk SessionKey) []byte {
	st := strconv.FormatInt(sk.AdShowTime, 10)
	at := strconv.FormatInt(sk.AdActionTime, 10)
	enc := []byte(sk.AdID + "&" + sk.ApMAC + "&" + sk.StationMAC + "&" + sk.StationIP +
		"&" + st + "&" + at)
	return enc
}

// EncodeSessionValue ecodes SessionValue to the bytes session value
// Bolt implementation
func EncodeSessionValue(sv SessionValue) ([]byte, error) {
	enc, err := json.Marshal(sv)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

// DecodeSessionKey decodes the bytes session key to SessionKey
// Bolt implementation
func DecodeSessionKey(sessionkey []byte) SessionKey {
	var key SessionKey
	item := strings.Split(string(sessionkey), "&")
	key.AdID = item[0]
	key.ApMAC = item[1]
	key.StationMAC = item[2]
	key.StationIP = item[3]
	key.AdShowTime, _ = strconv.ParseInt(item[4], 10, 64)
	key.AdActionTime, _ = strconv.ParseInt(item[5], 10, 64)
	return key
}

// DecodeSessionValue decodes the bytes session key to SessionValue
// Bolt implementation
func DecodeSessionValue(sessionvalue []byte) (SessionValue, error) {
	var value SessionValue
	err := json.Unmarshal(sessionvalue, &value)
	if err != nil {
		return value, err
	}
	return value, nil
}

// ByteSessionList returns list of ByteSessions
// Bolt implementation
func ByteSessionList(db *bolt.DB) (ByteSessions, error) {
	var err error
	session := new(ByteSession)
	var sessions ByteSessions
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Session"))
		sb := b.Bucket([]byte("Action")).Cursor()
		for k, v := sb.First(); k != nil; k, v = sb.Next() {
			session.Key = k
			session.Value = v
			sessions = append(sessions, *session)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not get the list of sessions %s", err)
	}
	return sessions, err
}
