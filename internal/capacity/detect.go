package capacity

import (
	"io/fs"
	"math"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
)

// Detect finds the effective process ceiling from Go, cgroups, or host memory.
func Detect() (int64, string) {
	return detect(os.DirFS("/"), debug.SetMemoryLimit(-1))
}

func detect(files fs.FS, goLimit int64) (int64, string) {
	limit, source := int64(math.MaxInt64), "fallback"
	consider := func(value int64, name string) {
		if value > 0 && value < limit {
			limit, source = value, name
		}
	}
	if goLimit < math.MaxInt64 {
		consider(goLimit, "gomemlimit")
	}
	if raw, err := fs.ReadFile(files, "proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 3 && fields[0] == "MemTotal:" {
				value, _ := strconv.ParseInt(fields[1], 10, 64)
				if value > 0 && value < math.MaxInt64/1024 {
					consider(value*1024, "host")
				}
			}
		}
	}
	// Inspect ancestors too: a child may be unlimited inside a limited parent.
	memberships, _ := fs.ReadFile(files, "proc/self/cgroup")
	mounts, _ := fs.ReadFile(files, "proc/self/mountinfo")
	for _, membership := range strings.Split(string(memberships), "\n") {
		parts := strings.SplitN(membership, ":", 3)
		if len(parts) != 3 {
			continue
		}
		v2 := parts[0] == "0" && parts[1] == ""
		if !v2 && !containsController(parts[1], "memory") {
			continue
		}
		for _, mount := range strings.Split(string(mounts), "\n") {
			columns := strings.SplitN(mount, " - ", 2)
			if len(columns) != 2 {
				continue
			}
			left, right := strings.Fields(columns[0]), strings.Fields(columns[1])
			if len(left) < 5 || len(right) < 3 {
				continue
			}
			filename := "memory.max"
			if v2 {
				if right[0] != "cgroup2" {
					continue
				}
			} else {
				if right[0] != "cgroup" || !containsController(right[2], "memory") {
					continue
				}
				filename = "memory.limit_in_bytes"
			}
			root, point := unescapeMount(left[3]), unescapeMount(left[4])
			member := path.Clean(parts[2])
			if member == "/" {
				member = root
			}
			if root != "/" && member != root && !strings.HasPrefix(member, root+"/") {
				continue
			}
			relative := strings.TrimPrefix(member, root)
			base := strings.TrimPrefix(path.Clean(point), "/")
			directory := path.Join(base, relative)
			for {
				raw, err := fs.ReadFile(files, path.Join(directory, filename))
				if err == nil {
					value, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
					consider(value, "cgroup")
				}
				if directory == base {
					break
				}
				parent := path.Dir(directory)
				if parent == directory || !strings.HasPrefix(parent+"/", base+"/") {
					break
				}
				directory = parent
			}
		}
	}
	if limit == math.MaxInt64 {
		return 1 << 30, "fallback"
	}
	return max(1024, limit), source
}

func containsController(value, controller string) bool {
	for _, item := range strings.Split(value, ",") {
		if item == controller {
			return true
		}
	}
	return false
}

func unescapeMount(value string) string {
	return strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\").Replace(value)
}
