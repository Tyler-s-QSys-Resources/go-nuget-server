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
	"os"
	"path"
	"strconv"
	"strings"
)

// Global Variables
var server *Server

func init() {
	// Loan config and init server
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
			log.Println("→ Expecting prefix:", server.URL.Path+"nupkg")

			// Perform Routing
			switch {
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`Packages`):
				servePackageFeed(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`FindPackagesById`):
				servePackageFeed(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`nupkg`):
				servePackageFile(&sw, r)
			case strings.HasPrefix(r.URL.String(), server.URL.Path+`files`):
				serveStaticFile(&sw, r, r.URL.String()[len(server.URL.Path+`files`):])
			case strings.HasPrefix(r.URL.String(), altFilePath):
				serveStaticFile(&sw, r, r.URL.String()[len(altFilePath):])
			}
		case http.MethodPut:
			// Bounce any request without write accees
			if accessLevel != accessReadWrite {
				sw.WriteHeader(http.StatusForbidden)
				return
			}

			// Route
			switch {
			case r.URL.String() == server.URL.Path:
				// Process Request
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
	p := ""
	// if port is set in URL string
	if server.URL.Port() != "" {
		p = ":" + server.URL.Port()
	}
	// If PORT EnvVar is set (Google Cloud Run environment)
	if os.Getenv("PORT") != "" {
		p = ":" + os.Getenv("PORT")
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

	// Local Variables
	var err error
	var b []byte
	var params = &packageParams{}
	var isMore bool

	// Identify & process function parameters if they exist
	if i := strings.Index(r.URL.Path, "("); i >= 0 { // Find opening bracket
		if j := strings.Index(r.URL.Path[i:], ")"); j >= 0 { // Find closing bracket
			params = newPackageParams(r.URL.Path[i+1 : i+j])
		}
	}

	// For /Packages() Route
	if strings.HasPrefix(r.URL.String(), server.URL.Path+`Packages`) {
		// If params are populated then this is a single entry requests
		if params.ID != "" && params.Version != "" {
			// Find the entry required
			npe, err := server.fs.GetPackageEntry(params.ID, params.Version)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Convert it to Bytes
			b = npe.ToBytes()
		} else {
			// Create a new Service Struct
			nf := NewNugetFeed("Packages", server.URL.String())

			// Split out weird filter formatting
			s := strings.SplitAfterN(r.URL.Query().Get("$filter"), " ", 3)

			// Create empty id string
			id := ""

			// If relevant, repopulate id with
			if strings.TrimSpace(s[0]) == "tolower(Id)" && strings.TrimSpace(s[1]) == "eq" {
				id = s[2]              // Assign to id
				id = id[1 : len(id)-1] // Remove quote marks
			}

			// If $skiptoke is supplied, form it into a package name
			startAfter := r.URL.Query().Get("$skiptoken")
			startAfter = strings.ReplaceAll(startAfter, `'`, ``)
			startAfter = strings.ReplaceAll(startAfter, `,`, `.`)

			// Populate Packages from FileStore (100 max)
			nf.Packages, isMore, err = server.fs.GetPackageFeedEntries(id, startAfter, 100)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Add link to next page if relevant
			if r.URL.Query().Get("$top") != "" && isMore {
				// Get the current $top, cast to Int
				t, err := strconv.Atoi(r.URL.Query().Get("$top"))
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				// Get a copy of the request URL
				u, err := url.Parse(r.URL.String())
				u.Host = server.URL.Hostname()
				u.Scheme = server.URL.Scheme
				// Get working copy of Query
				q := u.Query()
				// Update Values
				q.Del("$skip")
				q.Set("$top", strconv.Itoa(t-100))
				q.Set("$skiptoken", fmt.Sprintf(`'%s','%s'`, nf.Packages[len(nf.Packages)-1].Properties.ID, nf.Packages[len(nf.Packages)-1].Properties.Version))
				//Re-assign
				u.RawQuery = q.Encode()
				// Get un-encoded URL
				cleanURL, err := url.PathUnescape(u.String())
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				log.Println()
				// Add to feed
				nf.Link = append(nf.Link, &NugetLink{
					Rel:  "next",
					Href: cleanURL,
				})
			}

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Output Xml
			b = nf.ToBytes()
		}
	} else if strings.HasPrefix(r.URL.String(), server.URL.Path+`FindPackagesById`) {

		// Get ID from query
		id := r.URL.Query().Get("id") // Get Value
		id = id[1 : len(id)-1]        // Remove Quotes

		// Create a new Service Struct
		nf := NewNugetFeed("FindPackagesById", server.URL.String())

		// Populate Packages from FileStore
		nf.Packages, isMore, err = server.fs.GetPackageFeedEntries(id, "", 100)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Output Xml
		b = nf.ToBytes()
	}

	if len(b) == 0 {
		w.WriteHeader(404)
	} else {
		// Set Headers
		w.Header().Set("Content-Type", "application/atom+xml;type=feed;charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Write(b)
	}

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
