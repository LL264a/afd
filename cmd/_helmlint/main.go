//go:build ignore

package main

import (
	"fmt"
	"os"
	"text/template"

	"gopkg.in/yaml.v3"
)

func checkYAML(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("[%s] read error: %v\n", path, err)
		return
	}
	var out interface{}
	if err := yaml.Unmarshal(data, &out); err != nil {
		fmt.Printf("[%s] YAML INVALID: %v\n", path, err)
		return
	}
	fmt.Printf("[%s] YAML OK\n", path)
}

func checkTemplate(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("[%s] read error: %v\n", path, err)
		return
	}
	funcs := template.FuncMap{
		"include":      func(name string, data interface{}) string { return "" },
		"nindent":      func(n int, s string) string { return s },
		"toYaml":       func(v interface{}) string { return "" },
		"toJson":       func(v interface{}) string { return "" },
		"b64enc":       func(s string) string { return "" },
		"sha256sum":    func(s string) string { return "" },
		"randAlphaNum": func(n int) string { return "" },
		"quote":        func(s interface{}) string { return "" },
		"trim":         func(s string) string { return s },
		"contains":     func(s, substr string) bool { return false },
		"hasPrefix":    func(s, prefix string) bool { return false },
		"replace":      func(old, new, s string) string { return s },
		"trunc":        func(n int, s string) string { return s },
		"trimSuffix":   func(suffix, s string) string { return s },
		"default":      func(d, v interface{}) interface{} { return d },
		"empty":        func(v interface{}) bool { return false },
		"join":         func(sep string, elems []string) string { return "" },
		"list":         func(v ...interface{}) []interface{} { return v },
		"toToml":       func(v interface{}) string { return "" },
		"tpl":          func(s string, data interface{}) string { return s },
		"required":     func(msg string, v interface{}) (interface{}, error) { return v, nil },
		"int":          func(v interface{}) int { return 0 },
		"int64":        func(v interface{}) int64 { return 0 },
		"float64":      func(v interface{}) float64 { return 0 },
	}
	if _, err := template.New("t").Funcs(funcs).Parse(string(data)); err != nil {
		fmt.Printf("[%s] TEMPLATE SYNTAX INVALID: %v\n", path, err)
		return
	}
	fmt.Printf("[%s] TEMPLATE SYNTAX OK\n", path)
}

func main() {
	checks := []struct{ path, kind string }{
		{"deploy/helm/Chart.yaml", "yaml"},
		{"deploy/helm/values.yaml", "yaml"},
		{"deploy/helm/templates/_helpers.tpl", "tpl"},
		{"deploy/helm/templates/deployment.yaml", "tpl"},
		{"deploy/helm/templates/service.yaml", "tpl"},
		{"deploy/helm/templates/pvc.yaml", "tpl"},
		{"deploy/helm/templates/configmap.yaml", "tpl"},
		{"deploy/helm/templates/secret.yaml", "tpl"},
		{"deploy/helm/templates/serviceaccount.yaml", "tpl"},
		{"deploy/helm/templates/servicemonitor.yaml", "tpl"},
	}
	for _, c := range checks {
		switch c.kind {
		case "yaml":
			checkYAML(c.path)
		case "tpl":
			checkTemplate(c.path)
		}
	}
}
