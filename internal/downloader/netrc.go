package downloader

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// NetrcEntry represents a single entry in a .netrc file
type NetrcEntry struct {
	Machine  string
	Login    string
	Password string
	Account  string
}

// Netrc represents a parsed .netrc file
type Netrc struct {
	Entries []NetrcEntry
	Default *NetrcEntry
}

// LoadNetrc loads and parses a .netrc file
func LoadNetrc(path string) (*Netrc, error) {
	if path == "" {
		// Default locations
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, ".netrc")
			// On Windows, also check _netrc
			if _, err := os.Stat(path); err != nil {
				path = filepath.Join(home, "_netrc")
			}
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	netrc := &Netrc{}
	scanner := bufio.NewScanner(file)

	var current *NetrcEntry
	for scanner.Scan() {
		tokens := strings.Fields(scanner.Text())
		for i := 0; i < len(tokens); i++ {
			switch tokens[i] {
			case "machine":
				if current != nil {
					netrc.Entries = append(netrc.Entries, *current)
				}
				current = &NetrcEntry{}
				if i+1 < len(tokens) {
					current.Machine = tokens[i+1]
					i++
				}
			case "default":
				if current != nil {
					netrc.Entries = append(netrc.Entries, *current)
				}
				current = &NetrcEntry{}
				netrc.Default = current
			case "login":
				if current != nil && i+1 < len(tokens) {
					current.Login = tokens[i+1]
					i++
				}
			case "password":
				if current != nil && i+1 < len(tokens) {
					current.Password = tokens[i+1]
					i++
				}
			case "account":
				if current != nil && i+1 < len(tokens) {
					current.Account = tokens[i+1]
					i++
				}
			case "macdef":
				// Skip macdef blocks (multi-line, until blank line)
				// Just skip the token, next tokens on same line are macro name
			}
		}
	}

	if current != nil && current.Machine != "" {
		netrc.Entries = append(netrc.Entries, *current)
	}

	return netrc, scanner.Err()
}

// FindEntry finds the netrc entry for a given machine
func (n *Netrc) FindEntry(machine string) *NetrcEntry {
	for i := range n.Entries {
		if n.Entries[i].Machine == machine {
			return &n.Entries[i]
		}
	}
	return n.Default
}

// GetCredentials returns username and password for a given host from netrc
func (n *Netrc) GetCredentials(host string) (string, string) {
	entry := n.FindEntry(host)
	if entry == nil {
		return "", ""
	}
	return entry.Login, entry.Password
}
