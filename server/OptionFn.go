package main

import (
	"crypto/tls"
	"time"
)

type OptionFn func(s *Server)

func WithReadTimeout(readTimeout time.Duration) OptionFn {}

func WithTLSConfig(cfg *tls.Config) OptionFn {}

func WithWriteTimeout(writeTimeout time.Duration) OptionFn {}
