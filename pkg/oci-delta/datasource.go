package ocidelta

import (
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
)

type DataSource interface {
	tarpatch.DataSource
	Cleanup() error
}

type simpleDataSource struct {
	tarpatch.DataSource
}

func (s *simpleDataSource) Cleanup() error {
	return nil
}

func NewFilesystemDataSource(dir string) DataSource {
	return &simpleDataSource{tarpatch.NewFilesystemDataSource(dir)}
}
