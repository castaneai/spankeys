package spankeys

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	validDSNPattern = regexp.MustCompile("^projects/[^/]+/instances/[^/]+/databases/[^/]+$")
)

// DSN is name of Cloud Spanner database has the form projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID
type DSN string

func NewDSN(s string) (DSN, error) {
	if err := validDSN(s); err != nil {
		return DSN(s), err
	}
	return DSN(s), nil
}

func validDSN(dsn string) error {
	if matched := validDSNPattern.MatchString(dsn); !matched {
		return fmt.Errorf("database name %q should conform to pattern %q",
			dsn, validDSNPattern.String())
	}
	return nil
}

func (d DSN) Parent() string {
	return strings.Join(strings.Split(string(d), "/")[:4], "/")
}

func (d DSN) ProjectID() string {
	return strings.Split(string(d), "/")[1]
}

func (d DSN) InstanceID() string {
	return strings.Split(string(d), "/")[3]
}

func (d DSN) DatabaseID() string {
	return strings.Split(string(d), "/")[5]
}
