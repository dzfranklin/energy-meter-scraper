package main

import (
	"context"
	"energy-meter-scraper/glowapi"
	"github.com/influxdata/influxdb-client-go/v2"
	influxApi "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"
)

type resourceMeta struct {
	Name          string
	KWHResource   string
	PenceResource string
}

var resourcesOfInterest = []resourceMeta{
	{
		Name:          "electricity",
		KWHResource:   "24e7909c-c997-4506-9201-a57bd213148d",
		PenceResource: "cc4dbeca-e207-4618-95d2-bbddd120aa0b",
	},
	{
		Name:          "gas",
		KWHResource:   "5d594c77-f08a-4f6a-aac1-e086a5234b70",
		PenceResource: "0cb3f9b9-749b-4e7f-a1e0-c1d437b2e057",
	},
}

var glow *glowapi.API
var influxClient influxdb2.Client
var influxWrite influxApi.WriteAPIBlocking

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

	var glowErr error
	glow, glowErr = glowapi.Authenticate(glowUsername, glowPassword)
	if glowErr != nil {
		log.Fatal(glowErr)
	}
	slog.Info("authenticated with glow")

	for {
		slog.Info("requesting catchup")
		for _, meta := range resourcesOfInterest {
			for _, resourceID := range []string{meta.KWHResource, meta.PenceResource} {
				// This routinely fails
				catchupErr := glow.RequestResourceCatchup(resourceID)
				slog.Info("requested resource catchup", "resourceID", resourceID, "error", catchupErr)
			}
		}

		time.Sleep(5 * time.Minute)

		var points []*write.Point

		for _, meta := range resourcesOfInterest {
			tariffTime := time.Now()
			tariff, tariffErr := glow.Tariff(meta.KWHResource)
			if tariffErr != nil {
				log.Fatal(tariffErr)
			}
			points = append(points, write.NewPoint(
				"energy_tariff",
				map[string]string{"resource": meta.Name},
				map[string]any{
					"rate":           tariff.CurrentRates.Rate,
					"standingCharge": tariff.CurrentRates.StandingCharge,
				},
				tariffTime))

			kwhReadings, kwhReadingsErr := readResource(meta.KWHResource)
			penceReadings, penceReadingsErr := readResource(meta.PenceResource)
			if kwhReadingsErr != nil || penceReadingsErr != nil {
				log.Fatal(kwhReadingsErr, penceReadingsErr)
			}

			if len(kwhReadings.Data) != len(penceReadings.Data) {
				log.Fatal("expected same number of kwh and pence readings")
			}

			for i := 0; i < len(kwhReadings.Data); i++ {
				if kwhReadings.Data[i][0] != penceReadings.Data[i][0] {
					log.Fatal("expected corresponding readings to have same timestamp")
				}
				ts := kwhReadings.Data[i][0]

				kwhVal := kwhReadings.Data[i][1]
				penceVal := penceReadings.Data[i][1]

				points = append(points, write.NewPoint(
					"energy_usage",
					map[string]string{"resource": meta.Name, "period": "30m"},
					map[string]any{
						"kwh":   kwhVal,
						"pence": penceVal,
					},
					time.Unix(int64(ts), 0),
				))
			}
		}

		if err := influxWrite.WritePoint(context.Background(), points...); err != nil {
			log.Fatal(err)
		}
		slog.Info("wrote points to influx")

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

func readResource(id string) (*glowapi.ResourceReadings, error) {
	from, firstErr := glow.GetResourceFirstTime(id)
	if firstErr != nil {
		return nil, firstErr
	}

	to, lastErr := glow.GetResourceLastTime(id)
	if lastErr != nil {
		return nil, lastErr
	}

	cutoff := to.AddDate(0, 0, -8)
	if from.Before(cutoff) {
		from = cutoff
	}

	return glow.GetResourceReadings(glowapi.ResourceReadingsQuery{
		ID:       id,
		Period:   "PT30M",
		Function: "sum",
		From:     from,
		To:       to,
	})
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
