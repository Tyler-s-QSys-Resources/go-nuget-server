package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"fmt"
	"mime"
	"encoding/json"
	"sync"
	"time"

	nuspec "github.com/soloworks/go-nuspec"
)

type fileStoreLocal struct {
	rootDir  string
	packages []*NugetPackageEntry
	downloadCounts map[string]int
	countsPath string
	server   *Server
	lock	sync.RWMutex
}


func (fs *fileStoreLocal) Init(s *Server) error {

	// Set the Repo Path
	fs.rootDir = s.config.FileStore.RepoDIR
	fs.server = s

	// Create the package folder if required
	if _, err := os.Stat(fs.rootDir); os.IsNotExist(err) {
		log.Println("Creating Directory: ", fs.rootDir)
		err := os.MkdirAll(fs.rootDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// Load persisted download counts
	if err := fs.LoadDownloadCounts(); err != nil {
		log.Printf("Warning: could not load download counts: %v", err)
	}

	// Refresh Packages
	err := fs.RefeshPackages()
	if err != nil {
		return err
	}

	// Sync download counts into in-memory packages
	for _, p := range fs.packages {
		key := fmt.Sprintf("%s/%s", p.Properties.ID, p.Properties.Version)
		if count, ok := fs.downloadCounts[key]; ok {
			p.Properties.VersionDownloadCount.Value = count
		} else {
			p.Properties.VersionDownloadCount.Value = 0
		}
	}

	return nil
}

func (fs *fileStoreLocal) LoadDownloadCounts() error {
	fs.countsPath = filepath.Join(fs.rootDir, "downloads.json")
	fs.downloadCounts = make(map[string]int)

	data, err := ioutil.ReadFile(fs.countsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet; ignore
		}
		return err
	}

	return json.Unmarshal(data, &fs.downloadCounts)
}

func (fs *fileStoreLocal) SaveDownloadCounts() error {
	data, err := json.MarshalIndent(fs.downloadCounts, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fs.countsPath, data, 0644)
}

func (fs *fileStoreLocal) UpdateCountsInMemory() {
    for _, p := range fs.packages {
        key := fmt.Sprintf("%s/%s", p.Properties.ID, p.Properties.Version)
        if val, ok := fs.downloadCounts[key]; ok {
            p.Properties.VersionDownloadCount.Value = val
        } else {
            p.Properties.VersionDownloadCount.Value = 0
        }
    }
}

func (fs *fileStoreLocal) RecalculateLatestVersions() {
    // Map package ID to the highest version found
    latestVersions := make(map[string]string)
    
    // First pass: find highest version per package ID
    for _, p := range fs.packages {
        currentLatest, exists := latestVersions[p.Properties.ID]
        if !exists || compareVersions(p.Properties.Version, currentLatest) > 0 {
            latestVersions[p.Properties.ID] = p.Properties.Version
        }
    }
    
    // Second pass: mark packages accordingly
    for _, p := range fs.packages {
        latestVersion := latestVersions[p.Properties.ID]
        isLatest := compareVersions(p.Properties.Version, latestVersion) == 0
        p.Properties.IsLatestVersion = BoolProp{Value: isLatest, Type: "Edm.Boolean"}
        p.Properties.IsAbsoluteLatestVersion = BoolProp{Value: isLatest, Type: "Edm.Boolean"}
    }
}


func (fs *fileStoreLocal) RefeshPackages() error {

	// Read in all files in directory root
	IDs, err := ioutil.ReadDir(fs.rootDir)
	if err != nil {
		return err
	}

	// Loop through all directories (first level is lowercase IDs)
	for _, ID := range IDs {
		if ID.IsDir() {
			Vers, err := ioutil.ReadDir(filepath.Join(fs.rootDir, ID.Name()))
			if err != nil {
				return err
			}
			for _, Ver := range Vers {
				if Ver.IsDir() {
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

	// Sync download counts into in-memory packages after loading all packages
	for _, p := range fs.packages {
		key := fmt.Sprintf("%s/%s", p.Properties.ID, p.Properties.Version)
		if count, ok := fs.downloadCounts[key]; ok {
			p.Properties.VersionDownloadCount.Value = count
		} else {
			p.Properties.VersionDownloadCount.Value = 0
		}
	}

	// Recalculate latest version flags once after all packages are loaded
    fs.RecalculateLatestVersions()

	log.Printf("fs Loaded with %d Packages Found", len(fs.packages))

	return nil
}

func compareVersions(v1, v2 string) int {
    parse := func(v string) []int {
        parts := strings.Split(v, ".")
        nums := make([]int, len(parts))
        for i, p := range parts {
            n := 0
            fmt.Sscanf(p, "%d", &n)
            nums[i] = n
        }
        return nums
    }
    a := parse(v1)
    b := parse(v2)
    maxLen := len(a)
    if len(b) > maxLen {
        maxLen = len(b)
    }
    for i := 0; i < maxLen; i++ {
        var x, y int
        if i < len(a) {
            x = a[i]
        }
        if i < len(b) {
            y = b[i]
        }
        if x < y {
            return -1
        }
        if x > y {
            return 1
        }
    }
    return 0
}

func (fs *fileStoreLocal) LoadPackage(fp string) error {
	// Read package file
	content, err := ioutil.ReadFile(fp)
	if err != nil {
		return err
	}

	f, err := os.Stat(fp)
	if err != nil {
		return err
	}

	// Extract .nuspec and file list using shared function
	nsf, files, err := extractPackage(content)
	if err != nil {
		return fmt.Errorf("failed to extract nupkg: %w", err)
	}

	if nsf == nil {
		return fmt.Errorf("nuspec not found in nupkg")
	}

	// Create NugetPackageEntry
	p := NewNugetPackageEntry(nsf)
	p.Content.Src = fs.server.URL.String() + "nupkg/" + nsf.Meta.ID + "/" + nsf.Meta.Version

	// Set metadata timestamps
	modTime := f.ModTime().Format(zuluTimeLayout)
	p.Properties.Created.Value = modTime
	p.Properties.LastEdited.Value = modTime
	p.Properties.Published.Value = modTime
	p.Updated = modTime

	// Set hash and size
	hash := sha512.Sum512(content)
	p.Properties.PackageHash = hex.EncodeToString(hash[:])
	p.Properties.PackageHashAlgorithm = `SHA512`
	p.Properties.PackageSize.Value = len(content)
	p.Properties.PackageSize.Type = "Edm.Int64"

	// Insert into sorted list
	index := sort.Search(len(fs.packages), func(i int) bool { return fs.packages[i].Filename() > p.Filename() })
	x := NugetPackageEntry{}
	fs.packages = append(fs.packages, &x)
	copy(fs.packages[index+1:], fs.packages[index:])
	fs.packages[index] = p

	// Extract files that are inside "content/" in the nupkg to: <root>/<id>/<version>/content/
	contentDir := filepath.Join(fs.rootDir, strings.ToLower(nsf.Meta.ID), nsf.Meta.Version, "content")

	for filePath, data := range files {
		if strings.HasPrefix(filePath, "content/") && !zipFileIsDirectory(filePath) {
			// Remove all leading "content/" prefixes to avoid duplication
			relPath := filePath
			for strings.HasPrefix(relPath, "content/") {
				relPath = strings.TrimPrefix(relPath, "content/")
			}

			targetPath := filepath.Join(contentDir, filepath.FromSlash(relPath))

			if err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm); err != nil {
				return fmt.Errorf("failed to create content directory: %w", err)
			}
			if err := ioutil.WriteFile(targetPath, data, 0644); err != nil {
				return fmt.Errorf("failed to write content file: %w", err)
			}
		}
	}

	// After extracting content files successfully
	fs.RecalculateLatestVersions()

	return nil
}

func (fs *fileStoreLocal) RemovePackage(fn string) {
    fs.lock.Lock()
    defer fs.lock.Unlock()

    for i, p := range fs.packages {
        if p.Filename() == fn {
            fs.packages = append(fs.packages[:i], fs.packages[i+1:]...)
            break
        }
    }
    os.RemoveAll(filepath.Join(fs.rootDir, `content`, fn))

    fs.RecalculateLatestVersions()
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

	// Extract content folder into the version directory
	_, files, err := extractPackage(pkg)
	if err != nil {
		return false, fmt.Errorf("failed to extract package: %w", err)
	}

	for filePath, data := range files {
		if strings.HasPrefix(filePath, "content/") && !zipFileIsDirectory(filePath) {
			relPath := filePath
			// Strip all leading "content/" prefixes to avoid nesting
			for strings.HasPrefix(relPath, "content/") {
				relPath = strings.TrimPrefix(relPath, "content/")
			}

			destPath := filepath.Join(packageDir, "content", filepath.FromSlash(relPath))
			destDir := filepath.Dir(destPath)
			if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
				return false, fmt.Errorf("failed to create directory %s: %w", destDir, err)
			}
			if err := ioutil.WriteFile(destPath, data, 0644); err != nil {
				return false, fmt.Errorf("failed to write file %s: %w", destPath, err)
			}
		}
	}

	log.Printf("Package stored: %s %s", id, version)
	return true, nil
}

func zipFileIsDirectory(name string) bool {
	return strings.HasSuffix(name, "/") || path.Ext(name) == ""
}

func (fs *fileStoreLocal) GetPackageEntry(id string, ver string) (*NugetPackageEntry, error) {
	var match *NugetPackageEntry
	totalDownloads := 0

	for _, p := range fs.packages {
		if strings.EqualFold(p.Properties.ID, id) {
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

	return match, nil
}

func (fs *fileStoreLocal) GetPackageFeedEntries(id string, startAfter string, max int) ([]*NugetPackageEntry, bool, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// Aggregate total downloads per package ID
	downloadTotals := make(map[string]int)
	for _, p := range fs.packages {
		key := fmt.Sprintf("%s/%s", p.Properties.ID, p.Properties.Version)
		if count, ok := fs.downloadCounts[key]; ok {
			downloadTotals[p.Properties.ID] += count
		}
	}

	var packages []*NugetPackageEntry
	for _, p := range fs.packages {
		// Update per-version download count
		key := fmt.Sprintf("%s/%s", p.Properties.ID, p.Properties.Version)
		if count, ok := fs.downloadCounts[key]; ok {
			p.Properties.VersionDownloadCount.Value = count
		} else {
			p.Properties.VersionDownloadCount.Value = 0
		}

		// Set total download count per package ID
		if total, ok := downloadTotals[p.Properties.ID]; ok {
			p.Properties.DownloadCount.Value = total
		} else {
			p.Properties.DownloadCount.Value = 0
		}

		// Filter by ID if specified
		if id != "" && p.Properties.ID != id {
			continue
		}

		packages = append(packages, p)
	}

	// Sort packages by published date descending (newest first)
	sort.Slice(packages, func(i, j int) bool {
		ti, err1 := time.Parse(time.RFC3339, packages[i].Properties.Published.Value)
		tj, err2 := time.Parse(time.RFC3339, packages[j].Properties.Published.Value)
		if err1 != nil || err2 != nil {
			// If parsing fails, keep original order
			return false
		}
		return tj.Before(ti)
	})

	// Pagination logic
	start := 0
	if startAfter != "" {
		for i, p := range packages {
			if p.Properties.ID+"/"+p.Properties.Version == startAfter {
				start = i + 1
				break
			}
		}
	}

	end := start + max
	if end > len(packages) {
		end = len(packages)
	}

	hasMore := end < len(packages)

	return packages[start:end], hasMore, nil
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

	key := fmt.Sprintf("%s/%s", id, ver)
	fs.downloadCounts[key]++
	_ = fs.SaveDownloadCounts() // Optional: handle error or debounce

	for _, p := range fs.packages {
		if strings.EqualFold(p.Properties.ID, id) && p.Properties.Version == ver {
			p.Properties.VersionDownloadCount.Value = fs.downloadCounts[key]
			break
		}
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

