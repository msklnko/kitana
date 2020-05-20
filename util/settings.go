package util

import (
	"gopkg.in/yaml.v2"
	"os"
)

type Config struct {
	Database struct {
		Host string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"user"`
		Password string `yaml:"pass"`
	} `yaml:"database"`
}

var Configuration Config

func init() {
	f, err := os.Open("./settings.yaml")
	Er(err)

	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&Configuration)
	Er(err)
}
