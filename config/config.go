package config

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// Config contains configuration data for application
type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"user"`
		Password string `yaml:"pass"`
	} `yaml:"database"`
}

// Validate performs config validation
func (c Config) Validate() error {
	if len(c.Database.Host) == 0 {
		return errors.New("empty database host")
	}
	if len(c.Database.Port) == 0 {
		return errors.New("empty database port")
	}
	if len(c.Database.Username) == 0 {
		return errors.New("empty database username")
	}
	if len(c.Database.Password) == 0 {
		return errors.New("empty database password")
	}

	return nil
}

// MySQL returns MySQL configuration struct.
// You may call .FormatDSN method on it to obtain DSN
func (c Config) MySQL() *mysql.Config {
	return &mysql.Config{
		User:                 c.Database.Username,
		Passwd:               c.Database.Password,
		Addr:                 c.Database.Host + ":" + c.Database.Port,
		Net:                  "tcp",
		AllowNativePasswords: true,
	}
}
