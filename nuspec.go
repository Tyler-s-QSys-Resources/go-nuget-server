package nuspec

import (
	"bytes"
	"encoding/xml"
	"io"
	"io/ioutil"
	"os"
)

// File is used in the NuSpec struct
type File struct {
	Source string `xml:"src,attr"`
	Target string `xml:"target,attr"`
}

// Dependency is used in the File struct
type Dependency struct {
	ID      string `xml:"id,attr"`
	Version string `xml:"version,attr"`
}

// NuSpec Represents a .nuspec XML file found in the root of the .nupck files
type NuSpec struct {
	XMLName xml.Name `xml:"package"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Meta    struct { // MetaData
		ID         string `xml:"id"`
		Version    string `xml:"version"`
		Title      string `xml:"title,omitempty"`
		Authors    string `xml:"authors"`
		Owners     string `xml:"owners,omitempty"`
		LicenseURL string `xml:"licenseUrl,omitempty"`
		License    struct {
			Text string `xml:",chardata"`
			Type string `xml:"type,attr"`
		} `xml:"license,omitempty"`
		ProjectURL       string `xml:"projectUrl,omitempty"`
		IconURL          string `xml:"iconUrl,omitempty"`
		ReqLicenseAccept bool   `xml:"requireLicenseAcceptance"`
		Description      string `xml:"description"`
		ReleaseNotes     string `xml:"releaseNotes,omitempty"`
		Copyright        string `xml:"copyright,omitempty"`
		Summary          string `xml:"summary,omitempty"`
		Language         string `xml:"language,omitempty"`
		Tags             string `xml:"tags,omitempty"`
		Dependencies     struct {
			Dependency []Dependency `xml:"dependency"`
		} `xml:"dependencies,omitempty"`
	} `xml:"metadata"`
	Files struct {
		File []File `xml:"file"`
	} `xml:"files,omitempty"`
}

// New returns a populated skeleton for a Nuget Packages request (/Packages)
func New() *NuSpec {
	nsf := NuSpec{}
	nsf.Xmlns = `http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd`
	return &nsf
}

// FromFile reads a nuspec file from the file system
func FromFile(fn string) (*NuSpec, error) {

	// Open File
	xmlFile, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	// Read all file
	b, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		return nil, err
	}
	// Unmarshal into struct
	// Create empty struct
	var nsf NuSpec
	err = xml.Unmarshal(b, &nsf)
	if err != nil {
		return nil, err
	}

	return &nsf, nil
}

// FromBytes reads a nuspec file from a byte array
func FromBytes(b []byte) (*NuSpec, error) {
	nsf := NuSpec{}
	err := xml.Unmarshal(b, &nsf)
	if err != nil {
		return nil, err
	}
	return &nsf, nil
}

// FromReader reads a nuspec file from a byte array
func FromReader(r io.ReadCloser) (*NuSpec, error) {
	// Read contents of reader
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return FromBytes(b)
}

// ToBytes exports the nuspec to bytes in XML format
func (nsf *NuSpec) ToBytes() ([]byte, error) {
	var b bytes.Buffer
	// Unmarshal into XML
	output, err := xml.MarshalIndent(nsf, "", "  ")
	if err != nil {
		return nil, err
	}
	// Self-Close any empty XML elements (to match original Nuget output)
	// This assumes Indented Marshalling above, non Indented will break XML
	for bytes.Contains(output, []byte(`></`)) {
		i := bytes.Index(output, []byte(`></`))
		j := bytes.Index(output[i+1:], []byte(`>`))
		output = append(output[:i], append([]byte(` /`), output[i+j+1:]...)...)
	}
	// Write the XML Header
	b.WriteString(xml.Header)
	b.Write(output)
	return b.Bytes(), nil
}
