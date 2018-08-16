// Copyright 2013 Matthew Baird
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tableau4go

import (
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const content_type_header = "Content-Type"
const content_length_header = "Content-Length"
const auth_header = "X-Tableau-Auth"
const application_xml_content_type = "application/xml"
const POST = "POST"
const GET = "GET"
const DELETE = "DELETE"

var ErrDoesNotExist = errors.New("Does Not Exist")

// Debug api interactions. Set to try to enable debugging.
var Debug = false

// Signin signs in using the given username, password and contentURL
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Sign_In%3FTocPath%3DAPI%2520Reference%7C_____51
func (api *API) Signin(username, password string, contentURL string, userIDToImpersonate string) error {
	url := fmt.Sprintf("%s/api/%s/auth/signin", api.Server, api.Version)
	credentials := Credentials{Name: username, Password: password}
	if len(userIDToImpersonate) > 0 {
		credentials.Impersonate = &User{ID: userIDToImpersonate}
	}
	siteName := contentURL
	// this seems to have changed. If you are looking for the default site, you must pass
	// blank
	if api.OmitDefaultSiteName {
		if contentURL == api.DefaultSiteName {
			siteName = ""
		}
	}
	credentials.Site = &Site{ContentUrl: siteName}
	request := SigninRequest{Request: credentials}
	signInXML, err := request.XML()
	if err != nil {
		return err
	}
	payload := string(signInXML)
	headers := make(map[string]string)
	headers[content_type_header] = application_xml_content_type
	retval := AuthResponse{}
	err = api.makeRequest(url, POST, []byte(payload), &retval, headers, connectTimeOut, readWriteTimeout, "")
	if err == nil {
		api.AuthToken = retval.Credentials.Token
	}
	return err
}

// Signout signs the current user out of the tableau session.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Sign_Out%3FTocPath%3DAPI%2520Reference%7C_____52
func (api *API) Signout() error {
	url := fmt.Sprintf("%s/api/%s/auth/signout", api.Server, api.Version)
	headers := make(map[string]string)
	headers[content_type_header] = application_xml_content_type
	err := api.makeRequest(url, POST, nil, nil, headers, connectTimeOut, readWriteTimeout, "")
	return err
}

// ServerInfo returns server information for current Tableau server.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Server_Info%3FTocPath%3DAPI%2520Reference%7C__
func (api *API) ServerInfo() (ServerInfo, error) {
	// this call only works on apiVersion 2.4 and up
	url := fmt.Sprintf("%s/api/%s/serverinfo", api.Server, "2.4")
	headers := make(map[string]string)
	retval := ServerInfoResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.ServerInfo, err
}

// QuerySites returns a list of sites.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) QuerySites() ([]Site, error) {
	url := fmt.Sprintf("%s/api/%s/sites/", api.Server, api.Version)
	headers := make(map[string]string)
	retval := QuerySitesResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Sites.Sites, err
}

// QuerySite returns a site by it LUID.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) QuerySite(siteID string, includeStorage bool) (Site, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s", api.Server, api.Version, siteID)
	if includeStorage {
		url += fmt.Sprintf("?includeStorage=%v", includeStorage)
	}
	return api.querySite(url)
}

// QuerySiteByName returns a site by its name.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) QuerySiteByName(name string, includeStorage bool) (Site, error) {
	return api.querySiteByKey("name", name, includeStorage)
}

// QuerySiteByContentURL returns a site by its contentURL.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) QuerySiteByContentURL(contentURL string, includeStorage bool) (Site, error) {
	return api.querySiteByKey("contentURL", contentURL, includeStorage)
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) querySiteByKey(key, value string, includeStorage bool) (Site, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s?key=%s", api.Server, api.Version, value, key)
	if includeStorage {
		url += fmt.Sprintf("&includeStorage=%v", includeStorage)
	}
	return api.querySite(url)
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Sites%3FTocPath%3DAPI%2520Reference%7C_____40
func (api *API) querySite(url string) (Site, error) {
	headers := make(map[string]string)
	retval := QuerySiteResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Site, err
}

// QueryUserOnSite returns tne users currently on the given site.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_User_On_Site%3FTocPath%3DAPI%2520Reference%7C_____47
func (api *API) QueryUserOnSite(siteID, userID string) (User, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/users/%s", api.Server, api.Version, siteID, userID)
	headers := make(map[string]string)
	retval := QueryUserOnSiteResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.User, err
}

// QueryProjects returns the projects for the given site id.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Projects%3FTocPath%3DAPI%2520Reference%7C_____38
func (api *API) QueryProjects(siteID string) ([]Project, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/projects", api.Server, api.Version, siteID)
	headers := make(map[string]string)
	retval := QueryProjectsResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Projects.Projects, err
}

// QueryViews returns views for the given site.
func (api *API) QueryViews(siteID string) ([]View, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/views", api.Server, api.Version, siteID)
	headers := make(map[string]string)
	retval := QueryViewsResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Views.Views, err
}

// QueryWorkbookViews returns views for the given workbook
func (api *API) QueryWorkbookViews(siteID, workbookID string, values url.Values) ([]View, error) {
	params := values.Encode()
	if params != "" {
		params = "?" + params
	}
	url := fmt.Sprintf("%s/api/%s/sites/%s/workbooks/%s/views%s", api.Server, api.Version, siteID, workbookID, params)
	headers := make(map[string]string)
	retval := QueryViewsResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Views.Views, err
}

// QueryViewData returns csv data for the view
func (api *API) QueryViewData(siteID, viewID string) (*csv.Reader, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/views/%s/data", api.Server, api.Version, siteID, viewID)
	headers := make(map[string]string)
	// retVal := []byte{}
	retVal := csv.Reader{}
	err := api.makeRequest(url, GET, nil, &retVal, headers, 60*time.Second, 60*time.Second, "csv")
	return &retVal, err
}

// QueryWorkbooks returns workbooks for the given workbook
func (api *API) QueryWorkbooks(siteID string, values url.Values) ([]Workbook, error) {
	params := values.Encode()
	if params != "" {
		params = "?" + params
	}
	url := fmt.Sprintf("%s/api/%s/sites/%s/workbooks%s", api.Server, api.Version, siteID, params)
	headers := make(map[string]string)
	retval := QueryWorkbooksResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Workbooks.Workbooks, err
}

// GetProjectByName returns project by the given name
func (api *API) GetProjectByName(siteID, name string) (Project, error) {
	projects, err := api.QueryProjects(siteID)
	if err != nil {
		return Project{}, err
	}
	for _, project := range projects {
		if project.Name == name {
			return project, nil
		}
	}
	return Project{}, fmt.Errorf("Project Named '%s' Not Found", name)
}

// GetProjectByID returns project by the given ID
func (api *API) GetProjectByID(siteID, ID string) (Project, error) {
	projects, err := api.QueryProjects(siteID)
	if err != nil {
		return Project{}, err
	}
	for _, project := range projects {
		if project.ID == ID {
			return project, nil
		}
	}
	return Project{}, fmt.Errorf("Project with ID '%s' Not Found", ID)
}

// QueryDatasources returns DataSources for the given site ID.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Datasources%3FTocPath%3DAPI%2520Reference%7C_____33
func (api *API) QueryDatasources(siteID string) ([]Datasource, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/datasources", api.Server, api.Version, siteID)
	headers := make(map[string]string)
	retval := QueryDatasourcesResponse{}
	err := api.makeRequest(url, GET, nil, &retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval.Datasources.Datasources, err
}

// GetSiteID returns Sites by site name.
func (api *API) GetSiteID(siteName string) (string, error) {
	site, err := api.QuerySiteByName(siteName, false)
	if err != nil {
		return "", err
	}
	return site.ID, err
}

// CreateProject creates the given project
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Create_Project%3FTocPath%3DAPI%2520Reference%7C_____14
//POST /api/api-version/sites/site-id/projects
func (api *API) CreateProject(siteID string, project Project) (*Project, error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/projects", api.Server, api.Version, siteID)
	createProjectRequest := CreateProjectRequest{Request: project}
	xmlRep, err := createProjectRequest.XML()
	if err != nil {
		return nil, err
	}
	headers := make(map[string]string)
	headers[content_type_header] = application_xml_content_type
	createProjectResponse := CreateProjectResponse{}
	err = api.makeRequest(url, POST, xmlRep, &createProjectResponse, headers, connectTimeOut, readWriteTimeout, "")
	return &createProjectResponse.Project, err
}

// PublishTDS publishes the given datasource.
// http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Publish_Datasource%3FTocPath%3DAPI%2520Reference%7C_____31
func (api *API) PublishTDS(siteID string, tdsMetadata Datasource, fullTds string, overwrite bool) (retval *Datasource, err error) {
	return api.publishDatasource(siteID, tdsMetadata, fullTds, "tds", overwrite)
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Publish_Datasource%3FTocPath%3DAPI%2520Reference%7C_____31
func (api *API) publishDatasource(siteID string, tdsMetadata Datasource, datasource string, datasourceType string, overwrite bool) (retval *Datasource, err error) {
	url := fmt.Sprintf("%s/api/%s/sites/%s/datasources?datasourceType=%s&overwrite=%v", api.Server, api.Version, siteID, datasourceType, overwrite)
	payload := fmt.Sprintf("--%s\r\n", api.Boundary)
	payload += "Content-Disposition: name=\"request_payload\"\r\n"
	payload += "Content-Type: text/xml\r\n"
	payload += "\r\n"
	tdsRequest := DatasourceCreateRequest{Request: tdsMetadata}
	xmlRepresentation, err := tdsRequest.XML()
	if err != nil {
		return retval, err
	}
	payload += string(xmlRepresentation)
	payload += fmt.Sprintf("\r\n--%s\r\n", api.Boundary)
	payload += fmt.Sprintf("Content-Disposition: name=\"tableau_datasource\"; filename=\"%s.tds\"\r\n", tdsMetadata.Name)
	payload += "Content-Type: application/octet-stream\r\n"
	payload += "\r\n"
	payload += datasource
	payload += fmt.Sprintf("\r\n--%s--\r\n", api.Boundary)
	headers := make(map[string]string)
	headers[content_type_header] = fmt.Sprintf("multipart/mixed; boundary=%s", api.Boundary)
	err = api.makeRequest(url, POST, []byte(payload), retval, headers, connectTimeOut, readWriteTimeout, "")
	return retval, err
}

// DeleteDatasource deletes a datasource with the given ID.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Datasource%3FTocPath%3DAPI%2520Reference%7C_____15
func (api *API) DeleteDatasource(siteID string, datasourceID string) error {
	url := fmt.Sprintf("%s/api/%s/sites/%s/datasources/%s", api.Server, api.Version, siteID, datasourceID)
	return api.delete(url)
}

// DeleteProject deletes the project with the given ID.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Project%3FTocPath%3DAPI%2520Reference%7C_____17
func (api *API) DeleteProject(siteID string, projectID string) error {
	url := fmt.Sprintf("%s/api/%s/sites/%s/projects/%s", api.Server, api.Version, siteID, projectID)
	return api.delete(url)
}

// DeleteSite deletes the site with the given ID.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Project%3FTocPath%3DAPI%2520Reference%7C_____17
func (api *API) DeleteSite(siteID string) error {
	url := fmt.Sprintf("%s/api/%s/sites/%s", api.Server, api.Version, siteID)
	return api.delete(url)
}

// DeleteSiteByName deletes the site with the given name.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Site%3FTocPath%3DAPI%2520Reference%7C_____19
func (api *API) DeleteSiteByName(name string) error {
	return api.deleteSiteByKey("name", name)
}

// DeleteSiteByContentURL deletes the site with the given contentURL.
//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Site%3FTocPath%3DAPI%2520Reference%7C_____19
func (api *API) DeleteSiteByContentURL(contentURL string) error {
	return api.deleteSiteByKey("contentUrl", contentURL)
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Delete_Site%3FTocPath%3DAPI%2520Reference%7C_____19
func (api *API) deleteSiteByKey(key string, value string) error {
	url := fmt.Sprintf("%s/api/%s/sites/%s?key=%s", api.Server, api.Version, value, key)
	return api.delete(url)
}

func (api *API) delete(url string) error {
	headers := make(map[string]string)
	return api.makeRequest(url, DELETE, nil, nil, headers, connectTimeOut, readWriteTimeout, "")
}

// makeRequest calls the REST api with the given url, method and payload. The
// format param when not blank will deserialize for that format, defaulting to XML.
func (api *API) makeRequest(requestURL string, method string, payload []byte, result interface{}, headers map[string]string, cTimeout time.Duration, rwTimeout time.Duration, format string) error {
	if Debug {
		fmt.Printf("%s:%v\n", method, requestURL)
		if payload != nil {
			fmt.Printf("%v\n", string(payload))
		}
	}
	client := NewTimeoutClient(cTimeout, rwTimeout, false)
	var req *http.Request
	if len(payload) > 0 {
		var httpErr error
		req, httpErr = http.NewRequest(strings.TrimSpace(method), strings.TrimSpace(requestURL), bytes.NewBuffer(payload))
		if httpErr != nil {
			return httpErr
		}
		req.Header.Add(content_length_header, strconv.Itoa(len(payload)))
	} else {
		var httpErr error
		req, httpErr = http.NewRequest(strings.TrimSpace(method), strings.TrimSpace(requestURL), nil)
		if httpErr != nil {
			return httpErr
		}
	}
	if headers != nil {
		for header, headerValue := range headers {
			req.Header.Add(header, headerValue)
		}
	}
	if len(api.AuthToken) > 0 {
		if Debug {
			fmt.Printf("%s:%s\n", auth_header, api.AuthToken)
		}
		req.Header.Add(auth_header, api.AuthToken)
	}
	var httpErr error
	resp, httpErr := client.Do(req)
	if httpErr != nil {
		return httpErr
	}
	defer resp.Body.Close()
	body, readBodyError := ioutil.ReadAll(resp.Body)
	if Debug {
		fmt.Printf("t4g Response:%v\n", string(body))
	}
	if readBodyError != nil {
		return readBodyError
	}
	if resp.StatusCode == 404 {
		return ErrDoesNotExist
	}
	if resp.StatusCode >= 300 {
		tErrorResponse := ErrorResponse{}
		err := xml.Unmarshal(body, &tErrorResponse)
		if err != nil {
			return err
		}
		return tErrorResponse.Error
	}
	if result != nil {
		switch format {
		case "csv":
			*result.(*csv.Reader) = *csv.NewReader(bytes.NewReader(body))
		default:
			if err := xml.Unmarshal(body, &result); err != nil {
				return err
			}
		}
	}
	return nil
}
