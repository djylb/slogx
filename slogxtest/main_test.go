package slogxtest

import (
	"testing"

	"github.com/djylb/slogx/internal/leakcheck"
)

func TestMain(m *testing.M) { leakcheck.VerifyTestMain(m) }
