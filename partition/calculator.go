package partition

import (
	"errors"
	"time"

	"github.com/jinzhu/now"
	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/definition"
)

// Prefix prefix for partition name
const Prefix = "part"

// MonthFormat date format
const MonthFormat = "200601"

// DayFormat date format
const DayFormat = "20060102"

// NextOne Calculate next partition name and time
func NextOne(tp definition.Type, logger xray.Ray) (*string, *time.Time, error) {
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
	var name = Prefix + date.Format(MonthFormat)
	var limiter = now.New(date.AddDate(0, 1, 0)).BeginningOfMonth()
	return &name, &limiter
}

func nextDaily() (*string, *time.Time) {
	date := time.Now().UTC().AddDate(0, 0, 1)
	var name = Prefix + date.Format(DayFormat)
	var limiter = now.New(date.AddDate(0, 0, 1)).BeginningOfDay()
	return &name, &limiter
}

// NextSeveral Calculate next partition name and time
func NextSeveral(tp definition.Type, count int, withCurrent bool, logger xray.Ray) (map[string]time.Time, error) {
	logger.Debug("calculating current partition name and limiter, type :type", args.Type(tp))
	if tp == definition.Ml {
		return nextMonths(count, withCurrent), nil
	} else if tp == definition.Dl {
		return nextDays(count, withCurrent), nil
	} else {
		return nil, errors.New("not supported partition type " + tp.String())
	}
}

func nextMonths(count int, withCurrent bool) map[string]time.Time {
	var date = time.Now().UTC()
	if !withCurrent {
		date = date.AddDate(0, 1, 0)
	}
	result := make(map[string]time.Time)
	for i := 0; i <= count; i++ {
		name := Prefix + date.Format(MonthFormat)
		date = date.AddDate(0, 1, 0)
		result[name] = now.New(date).BeginningOfMonth()
	}
	return result
}

func nextDays(count int, withCurrent bool) map[string]time.Time {
	var date = time.Now().UTC()
	if !withCurrent {
		date = date.AddDate(0, 0, 1)
	}
	result := make(map[string]time.Time)
	for i := 0; i <= count; i++ {
		name := Prefix + date.Format(DayFormat)
		date = date.AddDate(0, 0, 1)
		result[name] = now.New(date).BeginningOfDay()
	}
	return result
}

// KeepAlive Calculate partition names to stay
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
	keepAlive = append(keepAlive, Prefix+date.Format(MonthFormat))

	iterates := count
	for iterates > 0 {
		date = date.AddDate(0, -1, 0)
		keepAlive = append(keepAlive, Prefix+date.Format(MonthFormat))
		iterates--
	}
	return keepAlive
}

func keepDaily(count int) []string {
	var keepAlive []string
	date := now.New(time.Now().UTC().AddDate(0, 0, 1)).BeginningOfDay()
	keepAlive = append(keepAlive, Prefix+date.Format(DayFormat))

	iterates := count
	for iterates > 0 {
		date = date.AddDate(0, 0, -1)
		keepAlive = append(keepAlive, Prefix+date.Format(DayFormat))
		iterates--
	}
	return keepAlive
}
