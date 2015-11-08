package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"io/ioutil"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
)

const authURL = "https://accounts.google.com/o/oauth2/auth"
const tokenURL = "https://accounts.google.com/o/oauth2/token"

// Client a big query client instance
type Client struct {
	pemPath           string
	token             *oauth2.Token
	service           *bigquery.Service
	allowLargeResults bool
	tempTableName     string
	flattenResults    bool
	PrintDebug        bool
}

// New instantiates a new client with the given params and return a reference to it
func New(pemPath string, options ...func(*Client) error) *Client {
	c := Client{
		pemPath: pemPath,
	}

	c.PrintDebug = false

	for _, option := range options {
		err := option(&c)
		if err != nil {
			return nil
		}
	}

	return &c
}

// connect - opens a new connection to bigquery,
// reusing the token if possible or regenerating a new auth token if required
func (c *Client) connect() (*bigquery.Service, error) {
	if c.token != nil {
		if !c.token.Valid() && c.service != nil {
			return c.service, nil
		}
	}

	// generate auth token and create service object
	//authScope := bigquery.BigqueryScope
	pemKeyBytes, err := ioutil.ReadFile(c.pemPath)
	if err != nil {
		panic(err)
	}

	t, err := google.JWTConfigFromJSON(
		pemKeyBytes,
		"https://www.googleapis.com/auth/bigquery")
	//t := jwt.NewToken(c.accountEmailAddress, bigquery.BigqueryScope, pemKeyBytes)
	client := t.Client(oauth2.NoContext)

	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	c.service = service
	return service, nil
}

// https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/api/export_data_to_cloud_storage.py
// https://godoc.org/code.google.com/p/google-api-go-client/bigquery/v2#Job
// https://cloud.google.com/bigquery/exporting-data-from-bigquery
func (c *Client) export(project_id, dataset_id, table_id, cloud_storage_path, format string) (*bigquery.Job, error) {
	job_data := fmt.Sprintf(`{
		"configuration": {
			"extract":{
				"sourceTable": {
					"projectId": "%s",
					"datasetId": "%s",
					"tableId": "%s",
				},
				"destinationUris": "%s",
				"destinationFormat": "%s",
				"compression": "GZIP"
			},
		}
	}`, project_id, dataset_id, table_id, cloud_storage_path, format)
	fmt.Println(string(job_data))
	var job bigquery.Job
	if err := json.Unmarshal([]byte(job_data), &job); err != nil {
		return nil, err
	}
	fmt.Println(job)
	return c.service.Jobs.Insert(project_id, &job).Do()
}

var (
	project_id         = flag.String("project_id", "", "project id")
	dataset_id         = flag.String("dataset_id", "", "dataset id")
	table_id           = flag.String("table_id", "", "table id")
	cloud_storage_path = flag.String("cloud_storage_path", "", "project id")
	format             = flag.String("format", "", "format")
)

func main() {
	flag.Parse()
	pemPath := "g.pem"
	c := New(pemPath)
	if _, err := c.connect(); err != nil {
		panic(err)
	}

	j, err := c.export(*project_id, *dataset_id, *project_id, *cloud_storage_path, *format)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	fmt.Printf("%v\n", j)
}
