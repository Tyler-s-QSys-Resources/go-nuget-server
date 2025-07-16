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

	nuspec "github.com/soloworks/go-nuspec"
)

type fileStoreLocal struct {
	rootDir  string
	packages []*NugetPackageEntry
}

func (fs *fileStoreLocal) Init(s *Server) error {

	// Set the Repo Path
	fs.rootDir = s.config.FileStore.RepoDIR

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

			// Read Entry into memory
			p = NewNugetPackageEntry(nsf)

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
	/*
		// Test for folder, if present bail, if not make it
		// Fixme: Broke this to get to compile
		packagePath := filepath.Join(c.FileStore.RepoDIR, strings.ToLower(nsf.Meta.ID), nsf.Meta.Version)
		if _, err := os.Stat(packagePath); !os.IsNotExist(err) {
			// Path already exists
			w.WriteHeader(http.StatusConflict)
			return
		}
		err = os.MkdirAll(packagePath, os.ModePerm)
		if err != nil {
			println(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Println("Creating Directory: ", packagePath)

		// Dump the .nupkg file in the same directory
		err = ioutil.WriteFile(filepath.Join(packagePath, strings.ToLower(nsf.Meta.ID)+"."+nsf.Meta.Version+".nupkg"), body, os.ModePerm)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}*/
	return false, nil
}

func (fs *fileStoreLocal) GetPackageEntry(id string, ver string) (*NugetPackageEntry, error) {

	return nil, nil
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

	return nil, "", nil
}
func (fs *fileStoreLocal) GetFile(f string) ([]byte, string, error) {

	return nil, "", nil
}

func (fs *fileStoreLocal) GetAccessLevel(key string) (access, error) {

	return accessReadWrite, nil
}
