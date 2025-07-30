package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"encoding/json"
	"time"
)

// Global Variables
var server *Server

func init() {
	// Load config and init server
	server = InitServer("nuget-server-config-local.json")
}

func main() {

	// Handling Routing
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		// Local Varibles
		var err error                                                          // Reusable error
		apiKey := ""                                                           // APIKey (populated if found in headers)
		accessLevel := accessDenied                                          // Access Level (defaults to denied)
		altFilePath := path.Join(`/F`, server.URL.Path, `api`, `v2`, `browse`) // Alternative API called by client

		// Create new statusWriter
		sw := statusWriter{ResponseWriter: w}

		// Check if this is NOT part of the Api Routing
		if !strings.HasPrefix(r.URL.Path, server.URL.Path) && !strings.HasPrefix(r.URL.Path, altFilePath) {
			f := path.Base(r.URL.Path)
			if f == "/" {
				f = "index.html"
			}
			serveStaticFile(&sw, r, path.Join("_www", f))
			goto End
		}

		// Open Access Routes (No ApiKey needed)
		switch r.Method {
		case http.MethodGet:
			switch {
			case r.URL.String() == server.URL.Path:
				serveRoot(&sw, r)
				goto End
			case r.URL.String() == server.URL.Path+`$metadata`:
				serveMetaData(&sw, r)
				goto End
			}
		}
		
		// Process Headers looking for API key (can't access direct as case may not match)
		for name, headers := range r.Header {
			// Grab ApiKey as it passes
			if strings.ToLower(name) == "x-nuget-apikey" {
				apiKey = headers[0]
			}
		}
		accessLevel, err = server.fs.GetAccessLevel(apiKey)
		if err != nil {
			sw.WriteHeader(http.StatusInternalServerError)
			goto End
		}
		// Bounce any unauthorised requests
		if accessLevel == accessDenied {
			sw.WriteHeader(http.StatusForbidden)
			goto End
		}

		log.Println("Route check — r.URL.String():", r.URL.String())
		log.Println("Route check — server.URL.Path:", server.URL.Path)


		// Restricted Routes
		switch r.Method {
		case http.MethodGet:
			log.Println("Routing Debug:")
			log.Println("→ r.URL.Path =", r.URL.Path)
			log.Println("→ server.URL.Path =", server.URL.Path)

			// Perform Routing
			switch {
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`Packages`):
				servePackageFeed(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`api/v2/Packages`):
				log.Println("API V2 Packages Route")
				servePackageFeed(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`FindPackagesById`):
				log.Println("FindPackagesById Route")
				servePackageFeed(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`nupkg`):
				servePackageFile(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`files`):
				serveStaticFile(&sw, r, r.URL.String()[len(server.URL.Path+`files`):])
			case strings.HasPrefix(r.URL.String(), altFilePath):
				serveStaticFile(&sw, r, r.URL.String()[len(altFilePath):])
			}
		case http.MethodPut:
			log.Println("PUT found!")
			if accessLevel != accessReadWrite {
				sw.WriteHeader(http.StatusForbidden)
				return
			}

			// Route
			switch {
			case r.URL.String() == server.URL.Path:
				// Process Request
				uploadPackage(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`api/v2/package/`):
				log.Println("API V2 Upload Package")
				uploadPackage(&sw, r)
			default:
				sw.WriteHeader(http.StatusNotFound)
				goto End
			}
		default:
			sw.WriteHeader(http.StatusNotFound)
			goto End
		}

	End:

		log.Println("Request::", sw.Status(), r.Method, r.URL.String())

		if server.config.Loglevel > 0 {
			log.Println("Request Headers:")
			if len(w.Header()) == 0 {
				log.Println("        None")
			} else {
				for name, headers := range r.Header {
					for _, h := range headers {
						// Log Key
						log.Println("        " + name + "::" + h)
					}
				}
			}

			log.Println("Response Headers:")
			if len(w.Header()) == 0 {
				log.Println("        None")
			} else {
				for name, headers := range w.Header() {
					for _, h := range headers {
						// Log Key
						log.Println("        " + name + "::" + h)
					}
				}
			}
		}
	})

	// Set port number (Defaults to 80)
	p := "" //DO not modify this value, if you need to use a different port, make sure it is set in the server.URL
	// if port is set in URL string
	if server.URL.Port() != "" {
		p = ":" + server.URL.Port()
	}

	// Log and Start server
	log.Println("Starting Server on ", server.URL.String()+p)
	log.Fatal(http.ListenAndServe(p, nil))
}

func serveRoot(w http.ResponseWriter, r *http.Request) {

	// Create a new Service Struct
	ns := NewNugetService(server.URL.String())
	b := ns.ToBytes()

	// Set Headers
	w.Header().Set("Content-Type", "application/xml;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))

	// Output Xml
	w.Write(b)
}

func serveMetaData(w http.ResponseWriter, r *http.Request) {

	// Set Headers
	w.Header().Set("Content-Type", "application/xml;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(server.MetaDataResponse)))

	// Output Xml
	w.Write(server.MetaDataResponse)
}

func serveStaticFile(w http.ResponseWriter, r *http.Request, fn string) {

	// Get the file from the FileStore
	b, c, err := server.fs.GetFile(fn)
	if err == ErrFileNotFound {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Set Headers
	w.Header().Set("Content-Type", c)
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))

	// Output Xml
	w.Write(b)
}

func servePackageFile(w http.ResponseWriter, r *http.Request) {

	log.Println("Serving Package File")
	// get the last two parts of the URL
	x := strings.Split(r.URL.String(), `/`)

	// Get the file
	b, t, err := server.fs.GetPackageFile(x[len(x)-2], x[len(x)-1])
	if err == ErrFileNotFound {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return

	}

	// Set header to fix filename on client side
	w.Header().Set("Cache-Control", "max-age=3600")
	w.Header().Set("Content-Disposition", `filename=`+x[len(x)-2]+x[len(x)-1]+".nupkg")
	w.Header().Set("Content-Type", t)
	// Serve up the file
	w.Write(b)
}

func servePackageFeed(w http.ResponseWriter, r *http.Request) {
	var err error
	var b []byte
	var params = &packageParams{}
	var isMore bool
	var nf *NugetFeed

	// Handle /FindPackagesById()?id='foo'
	if strings.HasPrefix(r.URL.Path, server.URL.Path+`FindPackagesById`) {
		id := strings.Trim(r.URL.Query().Get("id"), `'`)
		log.Println("FindPackagesById ID Param:", id)
		nf = NewNugetFeed("FindPackagesById", server.URL.String())

		// Update counts before fetching packages
		server.fs.UpdateCountsInMemory()

		log.Println("Calling GetPackageFeedEntries with ID:", id)
		nf.Packages, isMore, err = server.fs.GetPackageFeedEntries(id, "", 100)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if r.URL.Query().Get("$format") == "json" {
			renderJSONFeed(w, nf.Packages)
			return
		}

		b = nf.ToBytes()
	} else if strings.HasPrefix(r.URL.Path, server.URL.Path+`Packages`) ||
		strings.HasPrefix(r.URL.Path, server.URL.Path+`api/v2/Packages`) {

		if i := strings.Index(r.URL.Path, "("); i >= 0 {
			if j := strings.Index(r.URL.Path[i:], ")"); j >= 0 {
				params = newPackageParams(r.URL.Path[i+1 : i+j])
			}
		}

		if params.ID != "" && params.Version != "" {
			npe, err := server.fs.GetPackageEntry(params.ID, params.Version)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if r.URL.Query().Get("$format") == "json" {
				renderJSONFeed(w, []*NugetPackageEntry{npe})
				return
			}

			b = npe.ToBytes()
		} else {
			// Package list feed
			nf = NewNugetFeed("Packages", server.URL.String())

			s := strings.SplitAfterN(r.URL.Query().Get("$filter"), " ", 3)
			id := ""
			if len(s) == 3 && strings.TrimSpace(s[0]) == "tolower(Id)" && strings.TrimSpace(s[1]) == "eq" {
				id = s[2]
				id = strings.Trim(id, `'`)
			}

			startAfter := strings.ReplaceAll(strings.ReplaceAll(r.URL.Query().Get("$skiptoken"), `'`, ``), `,`, `.`)

			// Update counts before fetching packages
			server.fs.UpdateCountsInMemory()

			nf.Packages, isMore, err = server.fs.GetPackageFeedEntries(id, startAfter, 100)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if r.URL.Query().Get("$top") != "" && isMore {
				t, err := strconv.Atoi(r.URL.Query().Get("$top"))
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				u, err := url.Parse(r.URL.String())
				u.Host = server.URL.Hostname()
				u.Scheme = server.URL.Scheme

				q := u.Query()
				q.Del("$skip")
				q.Set("$top", strconv.Itoa(t-100))
				q.Set("$skiptoken", fmt.Sprintf(`'%s','%s'`,
					nf.Packages[len(nf.Packages)-1].Properties.ID,
					nf.Packages[len(nf.Packages)-1].Properties.Version))
				u.RawQuery = q.Encode()

				cleanURL, err := url.PathUnescape(u.String())
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				nf.Link = append(nf.Link, &NugetLink{
					Rel:  "next",
					Href: cleanURL,
				})
			}

			if r.URL.Query().Get("$format") == "json" {
				renderJSONFeed(w, nf.Packages)
				return
			}

			b = nf.ToBytes()
		}
	}

	if len(b) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/atom+xml;type=feed;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.Write(b)
}

func renderJSONFeed(w http.ResponseWriter, packages []*NugetPackageEntry) {
	type Metadata struct {
		ID          string `json:"id"`
		URI         string `json:"uri"`
		Type        string `json:"type"`
		EditMedia   string `json:"edit_media"`
		MediaSrc    string `json:"media_src"`
		ContentType string `json:"content_type"`
	}

	type PackageJson struct {
		Metadata        Metadata `json:"__metadata"`
		ID              string   `json:"Id"`
		Version         string   `json:"Version"`
		Authors         string   `json:"Authors"`
		Copyright       *string  `json:"Copyright"`
		Description     string   `json:"Description"`
		DownloadCount   string   `json:"DownloadCount"`
		IconURL         *string  `json:"IconUrl"`
		IsLatestVersion bool     `json:"IsLatestVersion"`
		Published       string   `json:"Published"`
		ProjectURL      string   `json:"ProjectUrl"`
		ReleaseNotes    string   `json:"ReleaseNotes"`
		Summary         string   `json:"Summary"`
		Tags            *string  `json:"Tags"`
		Title           string   `json:"Title"`
	}

	type ODataResponse struct {
		D struct {
			Results []PackageJson `json:"results"`
		} `json:"d"`
	}

	resp := ODataResponse{}

	for _, p := range packages {
		// Construct URLs
		packageID := url.PathEscape(p.Properties.ID)
		packageVersion := url.PathEscape(p.Properties.Version)
		baseURL := strings.TrimSuffix(server.URL.String(), "/")

		editUri := fmt.Sprintf("%s/api/v2/Packages(Id='%s',Version='%s')", baseURL, packageID, packageVersion)
		nupkgUrl := fmt.Sprintf("%s/nupkg/%s/%s", baseURL, packageID, packageVersion)
		mediaUrl := fmt.Sprintf("%s/api/v2/Packages(Id='%s',Version='%s')/$value", baseURL, packageID, packageVersion)

		// Format published date as /Date(milliseconds)/
		publishedMillis := parseDateToEpochMillis(p.Properties.Published.Value)
		published := fmt.Sprintf("/Date(%d)/", publishedMillis)

		// Optional fields
		var copyright *string
		if !p.Properties.Copyright.Null {
			copyright = &p.Properties.Copyright.Value
		}

		var iconURL *string
		if p.Properties.IconURL != "" {
			iconURL = &p.Properties.IconURL
		}

		var tags *string
		if p.Properties.Tags != "" {
			tags = &p.Properties.Tags
		}

		resp.D.Results = append(resp.D.Results, PackageJson{
			Metadata: Metadata{
				ID:          editUri,
				URI:         editUri,
				Type:        "MyGet.V2FeedPackage",
				EditMedia:   mediaUrl,
				MediaSrc:    nupkgUrl,
				ContentType: "binary/octet-stream",
			},
			ID:              p.Properties.ID,
			Version:         p.Properties.Version,
			Authors:         p.Author.Name,
			Copyright:       copyright,
			Description:     p.Properties.Description,
			DownloadCount:   strconv.Itoa(p.Properties.DownloadCount.Value),
			IconURL:         iconURL,
			IsLatestVersion: p.Properties.IsLatestVersion.Value,
			Published:       published,
			ProjectURL:      p.Properties.ProjectURL,
			ReleaseNotes:    p.Properties.ReleaseNotes.Value,
			Summary:         p.Summary.Text,
			Tags:            tags,
			Title:           p.Properties.Title,
		})
	}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonData)))
	w.Write(jsonData)
}

func parseDateToEpochMillis(dateStr string) int64 {
	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func uploadPackage(w http.ResponseWriter, r *http.Request) {

	log.Println("Putting Package into FileStore")

	// Parse Mime type
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Detect and Decode based on mime type
	if strings.HasPrefix(mediaType, "multipart/form-data") {
		// Get a multipart.Reader
		mr := multipart.NewReader(r.Body, params["boundary"])
		// Itterate over parts/files uploaded
		for {
			// Get he next part from the multipart.Reader, exit loop if no more
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			} else if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// Store the package file in byte array for use
			pkgFile, err := ioutil.ReadAll(p)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// Store the file
			_, err = server.fs.StorePackage(pkgFile)
			if err != nil {
				if strings.Contains(err.Error(), "already exists") {
					w.WriteHeader(http.StatusConflict)
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
				return
			}

			w.WriteHeader(http.StatusCreated)
		}
	}
}
