package glowapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"time"
)

// See
// - https://api.glowmarkt.com/api-docs/v0-1/resourcesys/#/
// - https://api.glowmarkt.com/api-docs/v0-1/vesys/#/
//
// The date time syntax is yyyy-mm-ddThh:mm:ss (i.e. 2017-09-19T10:00:00)

const (
	endpoint      = "https://api.glowmarkt.com/api/v0-1"
	applicationID = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
)

type API struct {
	token string
}

func Authenticate(username string, password string) (*API, error) {
	token, authErr := doAuth(username, password)
	if authErr != nil {
		return nil, authErr
	}

	return &API{token: token}, nil
}

func doAuth(username, password string) (string, error) {
	type request struct {
		Username      string `json:"username"`
		Password      string `json:"password"`
		ApplicationId string `json:"applicationId"`
	}

	type response struct {
		Valid     bool   `json:"valid"`
		Token     string `json:"token"`
		Exp       int    `json:"exp"`
		AccountId string `json:"accountId"`
	}

	reqBody, serErr := json.Marshal(request{
		Username:      username,
		Password:      password,
		ApplicationId: applicationID,
	})
	if serErr != nil {
		return "", serErr
	}

	resp, postErr := http.Post(endpoint+"/auth", "application/json", bytes.NewBuffer(reqBody))
	if postErr != nil {
		return "", postErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("auth rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return "", fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", readErr
	}

	var authResp response
	if deserErr := json.Unmarshal(respBody, &authResp); deserErr != nil {
		return "", deserErr
	}

	if !authResp.Valid {
		return "", errors.New("auth response without valid=True")
	}

	return authResp.Token, nil
}

type VirtualEntity struct {
	Clone         bool           `json:"clone"`
	Active        bool           `json:"active"`
	ApplicationId string         `json:"applicationId"`
	VeTypeId      string         `json:"veTypeId"`
	PostalCode    string         `json:"postalCode"`
	Attributes    map[string]any `json:"attributes"`
	Resources     []struct {
		ResourceId     string `json:"resourceId"`
		ResourceTypeId string `json:"resourceTypeId"`
		Name           string `json:"name"`
	} `json:"resources"`
	OwnerId   string    `json:"ownerId"`
	Name      string    `json:"name"`
	VeId      string    `json:"veId"`
	UpdatedAt time.Time `json:"updatedAt"`
	CreatedAt time.Time `json:"createdAt"`
}

type Resource struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Classifier  string `json:"classifier"`
	Storage     []struct {
		Type     string    `json:"type"`
		Sampling string    `json:"sampling"`
		Start    time.Time `json:"start"`
		Fields   []struct {
			FieldName string `json:"fieldName"`
			Unit      string `json:"unit"`
			Datatype  string `json:"datatype"`
			Negative  bool   `json:"negative"`
		} `json:"fields"`
	} `json:"storage"`
	DataSourceType     string `json:"dataSourceType"`
	ResourceTypeId     string `json:"resourceTypeId"`
	Active             bool   `json:"active"`
	ResourceId         string `json:"resourceId"`
	DataSourceUnitInfo struct {
		Shid string `json:"shid"`
	} `json:"dataSourceUnitInfo"`
	OwnerId string `json:"ownerId"`
}

type ResourceReadingsQuery struct {
	ID string `json:"id"`
	// the aggregation period of the readings, example, P1D for daily aggregation or PT30M for every 30 minutes
	Period string `json:"period"`
	// the aggregation function of the readings, example sum, avg, etc.
	Function string    `json:"function"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
}

type ResourceReadings struct {
	Name           string       `json:"name"`
	Classifier     string       `json:"classifier"`
	ResourceTypeId string       `json:"resourceTypeId"`
	ResourceId     string       `json:"resourceId"`
	Data           [][2]float64 `json:"data"`
	Units          string       `json:"units"`
}

type Tariff struct {
	From         Time `json:"from"`
	CurrentRates struct {
		StandingCharge float64 `json:"standingCharge"`
		Rate           float64 `json:"rate"`
	} `json:"currentRates"`
}

func (a *API) GetVirtualEntity(id string) (*VirtualEntity, error) {
	req, newReqErr := http.NewRequest("GET", endpoint+"/virtualentity/"+id, nil)
	if newReqErr != nil {
		return nil, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return nil, getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("GetVirtualEntity rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	var out VirtualEntity
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return nil, deserErr
	}

	return &out, nil
}

func (a *API) GetResource(id string) (*Resource, error) {
	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+id, nil)
	if newReqErr != nil {
		return nil, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return nil, getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("GetResource rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	var out Resource
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return nil, deserErr
	}

	return &out, nil
}

/*
	RequestResourceCatchup triggers an async request to DCC

Applicable to resources that are sourced from the DCC. This API will trigger an
asynchronous request to get the latest consumption readings from the DCC up to
the last complete half hour. The readings from the DCC are in half hour
intervals. To utilise the functionality of this API you need only make this
request once on the change of the half hour (preferably with a random delay of
up to 2 minutes).
*/
func (a *API) RequestResourceCatchup(id string) error {
	type response struct {
		Data struct {
			Valid bool `json:"valid"`
		} `json:"data"`
	}

	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+id+"/catchup", nil)
	if newReqErr != nil {
		return newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("RequestResourceCatchup rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}

	var out response
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return deserErr
	}

	if !out.Data.Valid {
		return errors.New("resource catchup response without valid=True")
	}

	return nil
}

func (a *API) GetResourceFirstTime(id string) (time.Time, error) {
	type response struct {
		Data struct {
			FirstTs int `json:"firstTs"`
		} `json:"data"`
	}

	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+id+"/first-time", nil)
	if newReqErr != nil {
		return time.Time{}, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return time.Time{}, getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("GetResourceFirstTime rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return time.Time{}, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return time.Time{}, readErr
	}

	var out response
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return time.Time{}, deserErr
	}

	return time.Unix(int64(out.Data.FirstTs), 0), nil
}

func (a *API) GetResourceLastTime(id string) (time.Time, error) {
	type response struct {
		Data struct {
			LastTs int `json:"lastTs"`
		}
	}

	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+id+"/last-time", nil)
	if newReqErr != nil {
		return time.Time{}, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return time.Time{}, getErr
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("GetResourceLastTime rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return time.Time{}, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return time.Time{}, readErr
	}

	var out response
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return time.Time{}, deserErr
	}

	return time.Unix(int64(out.Data.LastTs), 0), nil
}

func (a *API) GetResourceReadings(query ResourceReadingsQuery) (*ResourceReadings, error) {
	params := url.Values{}
	params.Set("period", query.Period)
	params.Set("function", query.Function)
	params.Set("from", (&Time{query.From}).String())
	params.Set("to", (&Time{query.To}).String())

	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+query.ID+"/readings?"+params.Encode(), nil)
	if newReqErr != nil {
		return nil, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return nil, getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("GetResourceReadings rejected", "httpStatus", resp.StatusCode, "body", string(body))

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	var out ResourceReadings
	if deserErr := json.Unmarshal(respBody, &out); deserErr != nil {
		return nil, deserErr
	}

	return &out, nil
}

func (a *API) Tariff(resourceID string) (*Tariff, error) {
	req, newReqErr := http.NewRequest("GET", endpoint+"/resource/"+resourceID+"/tariff", nil)
	if newReqErr != nil {
		return nil, newReqErr
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", a.token)
	req.Header.Set("applicationId", applicationID)

	resp, getErr := http.DefaultClient.Do(req)
	if getErr != nil {
		return nil, getErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Info("Tariff request failed", "httpStatus", resp.StatusCode, "body", string(body))

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	var data struct {
		Data []Tariff `json:"data"`
	}
	if deserErr := json.Unmarshal(respBody, &data); deserErr != nil {
		return nil, deserErr
	}

	if len(data.Data) == 0 {
		return nil, fmt.Errorf("no data in tariff response")
	}

	slices.SortFunc(data.Data, func(a, b Tariff) int {
		return a.From.Compare(b.From.Time)
	})
	tariff := &data.Data[len(data.Data)-1]

	return tariff, nil
}

type Time struct {
	time.Time
}

const glowTimeLayout = "2006-01-02T15:04:05"

func (t *Time) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	for _, layout := range []string{glowTimeLayout, "2006-01-02 15:04:05"} {
		parsed, err := time.Parse(layout, v)
		if err == nil {
			t.Time = parsed
			return nil
		}
	}

	return fmt.Errorf("failed to parse time: %s", string(b))
}

func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t *Time) String() string {
	return t.UTC().Format(glowTimeLayout)
}
