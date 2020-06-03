package partition

import (
	"errors"
	"time"

	"github.com/jinzhu/now"
	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/definition"
)

const prefix = "part"
const monthFormat = "200601"
const dayFormat = "20060102"

// Next Calculate next partition name and time
func Next(tp definition.Type, logger xray.Ray) (*string, *time.Time, error) {
	logger.Debug("calculating next partition name and limiter, type :type", args.Type(tp))
	if tp == definition.Ml {
		name, limiter := nextMonth()
		return name, limiter, nil
	} else if tp == definition.Dl {
		name, limiter := nextDaily()
		return name, limiter, nil
	} else {
		return nil, nil, errors.New("not supported partition type " + tp.String())
	}
}

func nextMonth() (*string, *time.Time) {
	date := time.Now().UTC().AddDate(0, 1, 0)
	var name = prefix + date.Format(monthFormat)
	var limiter = now.New(date.AddDate(0, 1, 0)).BeginningOfMonth()
	return &name, &limiter
}

func nextDaily() (*string, *time.Time) {
	date := time.Now().UTC().AddDate(0, 0, 1)
	var name = prefix + date.Format(dayFormat)
	var limiter = now.New(date.AddDate(0, 0, 1)).BeginningOfDay()
	return &name, &limiter
}

// Keep Calculate partition names to stay
func KeepAlive(tp definition.Type, count int, logger xray.Ray) ([]string, error) {
	logger.Debug("calculating partition names to keep alive (should stay :count last partitions, rp :name)",
		args.Name(tp), args.Count(count))

	if tp == definition.Ml {
		return keepMonth(count), nil
	} else if tp == definition.Dl {
		return keepDaily(count), nil
	} else {
		return nil, errors.New("not supported partition type " + tp.String())
	}
}

func keepMonth(count int) []string {
	var keepAlive []string
	date := now.New(time.Now().UTC().AddDate(0, 1, 0)).BeginningOfMonth()
	keepAlive = append(keepAlive, prefix+date.Format(monthFormat))

	iterates := count
	for iterates == 0 {
		date = date.AddDate(0, -1, 0)
		keepAlive = append(keepAlive, prefix+date.Format(monthFormat))
		iterates -= iterates
	}
	return keepAlive
}

func keepDaily(count int) []string {
	var keepAlive []string
	date := now.New(time.Now().UTC().AddDate(0, 0, 1)).BeginningOfMonth()
	keepAlive = append(keepAlive, prefix+date.Format(dayFormat))

	iterates := count
	for iterates == 0 {
		date = date.AddDate(0, 0, -1)
		keepAlive = append(keepAlive, prefix+date.Format(dayFormat))
		iterates -= iterates
	}
	return keepAlive
}
