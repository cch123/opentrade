// Package config loads service configuration from a YAML file plus environment
// variable overrides.
//
// Convention: services define their config struct, then call Load(path, &cfg)
// to populate it. Environment variables matching OPENTRADE_FOO_BAR will
// override the field FooBar in the struct (nested fields use __ as separator).
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const envPrefix = "OPENTRADE_"

// Load reads YAML from path (optional — empty path skips the file) and applies
// environment overrides into out. out must be a pointer to a struct.
func Load(path string, out any) error {
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("config: read %s: %w", path, err)
		}
		if err == nil {
			if err := yaml.Unmarshal(b, out); err != nil {
				return fmt.Errorf("config: parse %s: %w", path, err)
			}
		}
	}

	if err := applyEnvOverrides(out); err != nil {
		return fmt.Errorf("config: apply env: %w", err)
	}
	return nil
}

// applyEnvOverrides walks the struct and overrides fields from environment
// variables. Only string / int64 / bool are supported at this stage.
func applyEnvOverrides(out any) error {
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("out must be a non-nil pointer")
	}
	return walk("", v.Elem())
}

func walk(prefix string, v reflect.Value) error {
	if v.Kind() != reflect.Struct {
		return nil
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := field.Name
		if tag := field.Tag.Get("yaml"); tag != "" {
			name = strings.SplitN(tag, ",", 2)[0]
		}
		full := name
		if prefix != "" {
			full = prefix + "__" + name
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.Struct:
			if err := walk(full, fv); err != nil {
				return err
			}
		case reflect.String, reflect.Int, reflect.Int32, reflect.Int64, reflect.Bool:
			envName := envPrefix + strings.ToUpper(strings.ReplaceAll(full, "-", "_"))
			if raw, ok := os.LookupEnv(envName); ok {
				if err := setField(fv, raw); err != nil {
					return fmt.Errorf("%s: %w", envName, err)
				}
			}
		}
	}
	return nil
}

func setField(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Int, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	default:
		return fmt.Errorf("unsupported field kind %s", fv.Kind())
	}
	return nil
}
