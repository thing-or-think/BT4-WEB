//go:build !windows

package foldersync

func isTransientFSError(error) bool { return false }
