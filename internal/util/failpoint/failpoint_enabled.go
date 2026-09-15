//go:build failpoints

/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package failpoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Actions are armed via the FAILPOINTS environment variable, read once at
// first injection:
//
//	FAILPOINTS="apiserver/after-batch-dbstore=exit;processor/before-terminal-write=sleep(20000)"
//
// Supported actions:
//
//	exit         terminate the process with code 137 (simulates OOM kill)
//	exit(N)      terminate the process with code N
//	sleep(MS)    block the caller for MS milliseconds (holds a window open)
//
// Unknown or malformed actions are reported on stderr and ignored.

type action struct {
	kind    string
	value   int
	invalid string // raw text of an unparseable action, reported on trigger
}

// armed is read-only after first use.
var armed = sync.OnceValue(parseEnv)

func parseEnv() map[string]action {
	return parse(os.Getenv("FAILPOINTS"))
}

func parse(spec string) map[string]action {
	points := make(map[string]action)
	for _, entry := range strings.Split(spec, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, act, ok := strings.Cut(entry, "=")
		if !ok {
			points[entry] = action{invalid: entry}
			continue
		}
		points[strings.TrimSpace(name)] = parseAction(strings.TrimSpace(act))
	}
	return points
}

func parseAction(act string) action {
	switch {
	case act == "exit":
		return action{kind: "exit", value: 137}
	case strings.HasPrefix(act, "exit(") && strings.HasSuffix(act, ")"):
		code, err := strconv.Atoi(act[len("exit(") : len(act)-1])
		if err != nil {
			return action{invalid: act}
		}
		return action{kind: "exit", value: code}
	case strings.HasPrefix(act, "sleep(") && strings.HasSuffix(act, ")"):
		ms, err := strconv.Atoi(act[len("sleep(") : len(act)-1])
		if err != nil || ms < 0 {
			return action{invalid: act}
		}
		return action{kind: "sleep", value: ms}
	default:
		return action{invalid: act}
	}
}

// Inject evaluates the named failpoint and performs its armed action.
func Inject(name string) {
	act, ok := armed()[name]
	if !ok {
		return
	}
	if act.invalid != "" {
		fmt.Fprintf(os.Stderr, "failpoint %s: ignoring malformed action %q\n", name, act.invalid)
		return
	}
	fmt.Fprintf(os.Stderr, "failpoint %s: triggering %s(%d)\n", name, act.kind, act.value)
	switch act.kind {
	case "exit":
		os.Exit(act.value)
	case "sleep":
		time.Sleep(time.Duration(act.value) * time.Millisecond)
	}
}
