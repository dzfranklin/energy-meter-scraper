package main

import (
	"context"
	"energy-meter-scraper/glowapi"
	"fmt"
	"github.com/influxdata/influxdb-client-go/v2"
	influxApi "github.com/influxdata/influxdb-client-go/v2/api"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"
)

var resourcesOfInterest = map[string]string{
	"electricity_kwh_30m":   "24e7909c-c997-4506-9201-a57bd213148d",
	"electricity_pence_30m": "cc4dbeca-e207-4618-95d2-bbddd120aa0b",
	"gas_kwh_30m":           "5d594c77-f08a-4f6a-aac1-e086a5234b70",
	"gas_pence_30m":         "0cb3f9b9-749b-4e7f-a1e0-c1d437b2e057",
}

var glow *glowapi.API
var influxClient influxdb2.Client
var influxWrite influxApi.WriteAPIBlocking
var influxQuery influxApi.QueryAPI

func main() {
	glowUsername := "daniel@danielzfranklin.org"
	glowPassword := mustGetEnv("GLOW_PASSWORD")

	influxHost := mustGetEnv("INFLUX_HOST")
	influxToken := mustGetEnv("INFLUX_TOKEN")
	influxOrg := mustGetEnv("INFLUX_ORG")
	influxBucket := mustGetEnv("INFLUX_BUCKET")

	slog.Info("delaying start")
	sleepJitter(15 * time.Second)

	influxClient = influxdb2.NewClient(influxHost, influxToken)
	influxWrite = influxClient.WriteAPIBlocking(influxOrg, influxBucket)
	influxQuery = influxClient.QueryAPI(influxOrg)

	var glowErr error
	glow, glowErr = glowapi.Authenticate(glowUsername, glowPassword)
	if glowErr != nil {
		log.Fatal(glowErr)
	}
	slog.Info("authenticated with glow")

	for {
		sleepJitter(2 * time.Minute)

		for _, resourceID := range resourcesOfInterest {
			// This routinely fails
			catchupErr := glow.RequestResourceCatchup(resourceID)
			slog.Info("requested resource catchup", "resourceID", resourceID, "error", catchupErr)
		}

		time.Sleep(5 * time.Minute)

		for measurement, resourceID := range resourcesOfInterest {
			lastTimeStoredRows, lastTimeStoredErr := influxQuery.Query(context.Background(), fmt.Sprintf(`
from(bucket: "home")
|> range(start: 0, stop: now())
|> filter(fn: (r) => r["_measurement"] == "%s")
|> keep(columns: ["_time"])
|> last(column: "_time")`, measurement))
			if lastTimeStoredErr != nil {
				log.Fatal(lastTimeStoredErr)
			}
			lastTimeStoredRows.Next()

			var lastTimeStored time.Time
			if lastTimeStoredRows.Record() != nil {
				lastTimeStored = lastTimeStoredRows.Record().Time()
				slog.Info("read last time stored", "measurement", measurement, "time", lastTimeStored)
			}

			var fromTime time.Time

			if lastTimeStored.IsZero() {
				firstTime, firstErr := glow.GetResourceFirstTime(resourceID)
				if firstErr != nil {
					log.Fatal(firstErr)
				}
				fromTime = firstTime
			}

			cutoff := time.Now().AddDate(0, 0, -8)
			if fromTime.Before(cutoff) {
				slog.Info("limiting backfill", "resourceID", resourceID)
				fromTime = cutoff
			}

			lastTimeAvailable, lastErr := glow.GetResourceLastTime(resourceID)
			if lastErr != nil {
				log.Fatal(lastErr)
			}
			slog.Info("got last time available", "resourceID", resourceID, "lastTimeAvailable", lastTimeAvailable)

			if !lastTimeAvailable.After(lastTimeStored) {
				slog.Info("no new readings for resource", "measurement", measurement, "resourceID", resourceID)
				continue
			}

			readings, readingsErr := glow.GetResourceReadings(glowapi.ResourceReadingsQuery{
				ID:       resourceID,
				Period:   "PT30M",
				Function: "sum",
				From:     fromTime,
				To:       lastTimeAvailable,
			})
			if readingsErr != nil {
				log.Fatal(readingsErr)
			}
			slog.Info("got resource readings", "resourceID", resourceID, "count", len(readings.Data))

			for _, reading := range readings.Data {
				ts := time.Unix(int64(reading[0]), 0)
				val := reading[1]

				if !ts.After(lastTimeStored) {
					continue
				}

				slog.Info("inserting reading",
					"measurement", measurement,
					"resourceID", resourceID,
					"timestamp", ts,
					"value", val)
				writeErr := influxWrite.WriteRecord(context.Background(),
					fmt.Sprintf("%s value=%f %d", measurement, val, ts.UnixNano()))
				if writeErr != nil {
					log.Fatal(writeErr)
				}
			}
		}

		now := time.Now()
		nowMinute := now.Minute()
		var waitMinutes int
		if nowMinute < 30 {
			waitMinutes = 30 - nowMinute
		} else {
			waitMinutes = 60 - nowMinute
		}
		waitDur := time.Duration(waitMinutes) * time.Minute
		waitUntil := now.Add(waitDur)
		slog.Info("Waiting", "duration", waitDur, "until", waitUntil)
		time.Sleep(waitDur)
	}
}

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Missing required environment variable: %s", key)
	}
	return val
}

func sleepJitter(d time.Duration) {
	factor := rand.Float64()*(1.3-0.7) + 0.7
	time.Sleep(time.Duration(float64(d) * factor))
}
