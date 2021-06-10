package config

import "github.com/mono83/config"

// Read reads configuration file
func Read() (*Config, error) {
	var c Config
	src := config.Source{FileName: "settings.yaml"}
	if err := src.Read(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

// Configuration is global static configuration
// Not best practice
var Configuration Config

func init() {
	c, err := Read()
	if err != nil {
		panic(config.ExpandErrorMessage(err))
	}

	Configuration = *c
}
