package stream_common

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/Pallinder/go-randomdata"
	"github.com/go-redis/redis"
	"github.com/quipo/statsd"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"errors"
)

func GetHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "NoHost"
	}

	return hostname
}

func GetClientName() string {
	hostname := GetHostname()
	name := randomdata.SillyName()

	hostnameSplit := strings.Split(hostname, ".")

	return fmt.Sprintf("%s_%s", hostnameSplit[0], name)
}

func StartHeartbeat() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	})

	http.ListenAndServe(":5005", nil)
}

// GetStatsClient function
func GetStatsClient(address string, prefix string) *statsd.StatsdBuffer {
	statsdClient := statsd.NewStatsdClient(address, prefix)
	err := statsdClient.CreateSocket()

	if nil != err {
		log.Println(err)
		//os.Exit(1)
	}

	interval := time.Second * 2 // aggregate stats and flush every 2 seconds
	stats := statsd.NewStatsdBuffer(interval, statsdClient)
	//defer stats.Close()

	return stats
}

func GetRedisConnection(address string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     "", // no password set
		DB:           db,
		PoolSize:     10,
		PoolTimeout:  2 * time.Minute,
		IdleTimeout:  10 * time.Minute,
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 1 * time.Minute,
	})
}

func EndOf5Mins(timestamp time.Time) int64 {
	h := timestamp
	minute := h.Minute() - h.Minute() % 5
	return time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		minute, 0, 0, time.UTC).Add(5*time.Minute).Unix()
}

func EndOfPrevious5Mins(timestamp time.Time) int64 {
	h := timestamp
	minute := h.Minute() - h.Minute()%5
	return time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		minute, 0, 0, time.UTC).Unix()
}

func GetInt(val interface{}) int {
	floatVal, _ := val.(float64)
	return round(floatVal)
}

func round(val float64) int {
	if val < 0 {
		return int(val - 0.5)
	}
	return int(val + 0.5)
}

// ThreatIQ ////////////////////////////////////////////////////////////////////
func EndOfHour() int64 {
	h := time.Now().UTC().Add(time.Hour)
	return time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		0, 0, 0, time.UTC).Unix()
}

func EndOfPreviousHour() int64 {
	h := time.Now().UTC()
	return time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		0, 0, 0, time.UTC).Unix()
}

func Get5mWindow(ts time.Time) (int64, int64, error) {

	minute := ts.Minute() - ts.Minute() % 5

	fmt.Println(minute)

	tsPrevious5m := time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(),
		minute, 0, 0, time.UTC).Unix()

	fmt.Println()

	tsCurrent5m := time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(),
		minute, 0, 0, time.UTC).Add(5*time.Minute).Unix()
	fmt.Println(tsCurrent5m)

	tsNext5m := time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(),
		minute, 0, 0, time.UTC).Add(10*time.Minute).Unix()
	fmt.Println(tsNext5m)

	currentTime := time.Now().UTC().Unix()
	fmt.Println(currentTime)

	fmt.Println(tsNext5m - currentTime)

	if tsNext5m - currentTime > 0 {
		return tsPrevious5m, tsCurrent5m, nil
	}

	return time.Now().UTC().Unix(), time.Now().UTC().Unix(),
		errors.New("timestamp is out of valid range")
}

func Get5MinTimeframe20MinBack() (int64, int64) {
	h := time.Now().UTC()
	minute := h.Minute() - h.Minute()%5

	endOfPrev5Mins := time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		minute, 0, 0, time.UTC)

	t1 := endOfPrev5Mins.Add(- 20 * time.Minute)
	t2 := endOfPrev5Mins.Add(- 15 * time.Minute)

	return t1.Unix(), t2.Unix()
}

func Get5MinTimeframe80MinBack() (int64, int64) {
	h := time.Now().UTC()
	minute := h.Minute() - h.Minute()%5

	endOfPrev5Mins := time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		minute, 0, 0, time.UTC)

	t1 := endOfPrev5Mins.Add(- 80 * time.Minute)
	t2 := endOfPrev5Mins.Add(- 75 * time.Minute)

	return t1.Unix(), t2.Unix()
}

func Get1HourTimeframe91MinBack() (int64, int64) {
	h := time.Now().UTC()

	endOfPrevMin := time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		31, 0, 0, time.UTC)

	t1 := endOfPrevMin.Add(- 91 * time.Minute)
	t2 := endOfPrevMin.Add(- 31 * time.Minute)

	return t1.Unix(), t2.Unix()
}

func Get1HourTimeframe106MinBack() (int64, int64) {
	h := time.Now().UTC()

	endOfPrevMin := time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		46, 0, 0, time.UTC)

	t1 := endOfPrevMin.Add(- 106 * time.Minute)
	t2 := endOfPrevMin.Add(- 46 * time.Minute)

	return t1.Unix(), t2.Unix()
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func GetPeriodStart1h() time.Time {
	h := time.Now().UTC()

	endOfPrev5Mins := time.Date(h.Year(), h.Month(), h.Day(), h.Hour(),
		0, 0, 0, time.UTC)

	return endOfPrev5Mins.Add(- 2 * time.Hour).Add(-1 * time.Second)
}