package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"fmt"
	"mime"

	nuspec "github.com/soloworks/go-nuspec"
)

type fileStoreLocal struct {
	rootDir  string
	packages []*NugetPackageEntry
	server   *Server // Add this
}


func (fs *fileStoreLocal) Init(s *Server) error {

	// Set the Repo Path
	fs.rootDir = s.config.FileStore.RepoDIR
	fs.server = s

	// Create the package folder if requried
	if _, err := os.Stat(fs.rootDir); os.IsNotExist(err) {
		// Path already exists
		log.Println("Creating Directory: ", fs.rootDir)
		err := os.MkdirAll(fs.rootDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// Refresh Packages
	err := fs.RefeshPackages()
	if err != nil {
		return err
	}

	// Return repo
	return nil
}

func (fs *fileStoreLocal) RefeshPackages() error {

	// Read in all files in directory root
	IDs, err := ioutil.ReadDir(fs.rootDir)
	if err != nil {
		return err
	}

	// Loop through all directories (first level is lowercase IDs)
	for _, ID := range IDs {
		// Check if this is a directory
		if ID.IsDir() {
			// Search files in directory (second level is versions)
			Vers, err := ioutil.ReadDir(filepath.Join(fs.rootDir, ID.Name()))
			if err != nil {
				return err
			}
			for _, Ver := range Vers {
				// Check if this is a directory
				if Ver.IsDir() {
					// Create full filepath
					fp := filepath.Join(fs.rootDir, ID.Name(), Ver.Name(), ID.Name()+"."+Ver.Name()+".nupkg")
					if _, err := os.Stat(fp); os.IsNotExist(err) {
						log.Println("Not a nupkg directory")
						break
					}
					err = fs.LoadPackage(fp)
					if err != nil {
						log.Println("Error: Cannot load package")
						log.Println(err)
						break
					}
				}
			}
		}
	}

	log.Printf("fs Loaded with %d Packages Found", len(fs.packages))

	return nil
}

func (fs *fileStoreLocal) LoadPackage(fp string) error {

	// Open and read in the file (Is a Zip file under the hood)
	content, err := ioutil.ReadFile(fp)
	if err != nil {
		return err
	}

	f, err := os.Stat(fp)
	if err != nil {
		return err
	}

	// Set up a zipReader
	zipReader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return err
	}

	// NugetPackage Object
	var p *NugetPackageEntry

	// Find and Process the .nuspec file
	for _, zipFile := range zipReader.File {
		// If this is the root .nuspec file read it into a NewspecFile structure
		if filepath.Dir(zipFile.Name) == "." && filepath.Ext(zipFile.Name) == ".nuspec" {
			// Marshall XML into Structure
			rc, err := zipFile.Open()
			if err != nil {
				return err
			}
			b, err := ioutil.ReadAll(rc)
			if err != nil {
				return err
			}
			// Read into NuspecFile structure
			nsf, err := nuspec.FromBytes(b)
			if err != nil {
				log.Println("Error parsing nuspec:", err)
				return err
			}

			// Read Entry into memory
			p = NewNugetPackageEntry(nsf)


			p.Content.Src = fs.server.URL.String() + "nupkg/" + nsf.Meta.ID + "/" + nsf.Meta.Version

			// Set Updated to match file
			p.Properties.Created.Value = f.ModTime().Format(zuluTimeLayout)
			p.Properties.LastEdited.Value = f.ModTime().Format(zuluTimeLayout)
			p.Properties.Published.Value = f.ModTime().Format(zuluTimeLayout)
			p.Updated = f.ModTime().Format(zuluTimeLayout)
			// Get and Set file hash
			h := sha512.Sum512(content)
			p.Properties.PackageHash = hex.EncodeToString(h[:])
			p.Properties.PackageHashAlgorithm = `SHA512`
			p.Properties.PackageSize.Value = len(content)
			p.Properties.PackageSize.Type = "Edm.Int64"
			// Determine if this is the latest version for this package ID
			latest := true
			for _, existing := range fs.packages {
				if existing.ID == p.ID {
					// Lexicographic comparison — works for consistent version formatting
					if existing.Properties.Version > p.Properties.Version {
						latest = false
						break
					}
				}
			}

			// Assign using Property[bool]
			p.Properties.IsLatestVersion = BoolProp{Value: latest, Type: "Edm.Boolean"}
			p.Properties.IsAbsoluteLatestVersion = BoolProp{Value: latest, Type: "Edm.Boolean"}


			// Insert this into the array in order
			index := sort.Search(len(fs.packages), func(i int) bool { return fs.packages[i].Filename() > p.Filename() })
			x := NugetPackageEntry{}
			fs.packages = append(fs.packages, &x)
			copy(fs.packages[index+1:], fs.packages[index:])
			fs.packages[index] = p
		}
	}

	return nil
}

func (fs *fileStoreLocal) RemovePackage(fn string) {
	// Remove the Package from the Map
	for i, p := range fs.packages {
		if p.Filename() == fn {
			fs.packages = append(fs.packages[:i], fs.packages[i+1:]...)
		}
	}
	// Delete the contents directory
	os.RemoveAll(filepath.Join(fs.rootDir, `content`, fn))
}

func (fs *fileStoreLocal) StorePackage(pkg []byte) (bool, error) {
	// Open nupkg as zip reader
	zipReader, err := zip.NewReader(bytes.NewReader(pkg), int64(len(pkg)))
	if err != nil {
		return false, fmt.Errorf("invalid nupkg file: %w", err)
	}

	var nsf *nuspec.NuSpec
	// Find the .nuspec file
	for _, zipFile := range zipReader.File {
		if filepath.Ext(zipFile.Name) == ".nuspec" && filepath.Dir(zipFile.Name) == "." {
			rc, err := zipFile.Open()
			if err != nil {
				return false, fmt.Errorf("error opening nuspec: %w", err)
			}
			defer rc.Close()

			nuspecData, err := ioutil.ReadAll(rc)
			if err != nil {
				return false, fmt.Errorf("error reading nuspec: %w", err)
			}

			nsf, err = nuspec.FromBytes(nuspecData)
			if err != nil {
				return false, fmt.Errorf("error parsing nuspec: %w", err)
			}
			break
		}
	}

	if nsf == nil {
		return false, fmt.Errorf("nuspec file not found in package")
	}

	// Build the package path
	id := strings.ToLower(nsf.Meta.ID)
	version := nsf.Meta.Version
	packageDir := filepath.Join(fs.rootDir, id, version)
	nupkgFilename := fmt.Sprintf("%s.%s.nupkg", id, version)
	nupkgPath := filepath.Join(packageDir, nupkgFilename)

	// Check if already exists
	if _, err := os.Stat(nupkgPath); err == nil {
		return false, fmt.Errorf("package already exists: %s", nupkgPath)
	}

	// Create directory
	if err := os.MkdirAll(packageDir, os.ModePerm); err != nil {
		return false, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write the .nupkg file
	if err := ioutil.WriteFile(nupkgPath, pkg, 0644); err != nil {
		return false, fmt.Errorf("failed to write nupkg: %w", err)
	}

	// Load it into memory
	if err := fs.LoadPackage(nupkgPath); err != nil {
		return false, fmt.Errorf("failed to load package: %w", err)
	}

	log.Printf("Package stored: %s %s", id, version)
	return true, nil
}

func (fs *fileStoreLocal) GetPackageEntry(id string, ver string) (*NugetPackageEntry, error) {
	var match *NugetPackageEntry
	var latestVer string
	totalDownloads := 0

	for _, p := range fs.packages {
		if strings.EqualFold(p.Properties.ID, id) {
			// Track latest version for this ID
			if latestVer == "" || p.Properties.Version > latestVer {
				latestVer = p.Properties.Version
			}

			// Track total downloads for this ID
			totalDownloads += p.Properties.VersionDownloadCount.Value

			// Match target version
			if p.Properties.Version == ver {
				match = p
			}
		}
	}

	// If not found, return error to trigger 404 upstream
	if match == nil {
		return nil, fmt.Errorf("package not found")
	}

	// Update values like GCP does
	match.Properties.DownloadCount.Value = totalDownloads
	match.Properties.IsLatestVersion.Value = latestVer == ver
	match.Properties.IsAbsoluteLatestVersion.Value = latestVer == ver

	return match, nil
}

func (fs *fileStoreLocal) GetPackageFeedEntries(id string, startAfter string, max int) ([]*NugetPackageEntry, bool, error) {

	var entries []*NugetPackageEntry
	var startCollecting bool = (startAfter == "")
	var count int

	for _, p := range fs.packages {
		// Filter by package ID if provided
		if id != "" && p.ID != id {
			continue
		}

		// Skip until we reach the `startAfter` entry
		if !startCollecting {
			if p.Filename() == startAfter {
				startCollecting = true
			}
			continue
		}

		// Add the entry
		entries = append(entries, p)
		count++

		// Stop if we’ve reached the max requested
		if max > 0 && count >= max {
			break
		}
	}

	// Determine if there are more entries after this page
	hasMore := false
	if max > 0 && (count == max) && len(entries) > 0 {
		last := entries[len(entries)-1].Filename()
		for _, p := range fs.packages {
			if id != "" && p.ID != id {
				continue
			}
			if p.Filename() == last {
				startCollecting = true
				continue
			}
			if startCollecting {
				hasMore = true
				break
			}
		}
	}

	return entries, hasMore, nil
}

func (fs *fileStoreLocal) GetPackageFile(id string, ver string) ([]byte, string, error) {
	// Construct full path to nupkg file
	filename := filepath.Join(fs.rootDir, id, ver, fmt.Sprintf("%s.%s.nupkg", id, ver))

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", ErrFileNotFound
		}
		return nil, "", err
	}

	return content, "application/octet-stream", nil
}



func (fs *fileStoreLocal) GetFile(f string) ([]byte, string, error) {
	fullPath := filepath.Join(fs.rootDir, f)

	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return nil, "", ErrFileNotFound
	}

	// Detect content type from extension
	contentType := mime.TypeByExtension(filepath.Ext(fullPath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return data, contentType, nil
}

func (fs *fileStoreLocal) GetAccessLevel(key string) (access, error) {
	cfg := fs.server.config.FileStore.APIKeys

	// No keys defined — open server
	if len(cfg.ReadOnly) == 0 && len(cfg.ReadWrite) == 0 {
		return accessReadWrite, nil
	}

	// If any ReadOnly keys exist, all access requires a key
	if len(cfg.ReadOnly) > 0 {
		for _, k := range cfg.ReadWrite {
			if k == key {
				return accessReadWrite, nil
			}
		}
		for _, k := range cfg.ReadOnly {
			if k == key {
				return accessReadOnly, nil
			}
		}
		return accessDenied, fmt.Errorf("unauthorized")
	}

	// No ReadOnly keys, only ReadWrite keys: read is open, write requires a key
	for _, k := range cfg.ReadWrite {
		if k == key {
			return accessReadWrite, nil
		}
	}
	return accessReadOnly, nil
}

