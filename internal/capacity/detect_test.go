package capacity

import (
	"math"
	"testing"
	"testing/fstest"
)

func TestDetectCapacity(t *testing.T) {
	for _, tc := range []struct {
		name, membership, mount, filename, value string
		soft, want                               int64
		source                                   string
	}{
		{"v2-parent", "0::/pod/child", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/pod/memory.max", "104857600", math.MaxInt64, 100 << 20, "cgroup"},
		{"v1", "1:cpu,memory:/pod", "1 0 0:1 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory", "sys/fs/cgroup/memory/pod/memory.limit_in_bytes", "209715200", math.MaxInt64, 200 << 20, "cgroup"},
		{"namespaced", "0::/", "1 0 0:1 /pod /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "104857600", math.MaxInt64, 100 << 20, "cgroup"},
		{"soft", "0::/", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "104857600", 40 << 20, 40 << 20, "gomemlimit"},
		{"unlimited", "0::/", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "max", math.MaxInt64, 1 << 30, "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := fstest.MapFS{}
			files["proc/self/cgroup"] = &fstest.MapFile{Data: []byte(tc.membership)}
			files["proc/self/mountinfo"] = &fstest.MapFile{Data: []byte(tc.mount)}
			files[tc.filename] = &fstest.MapFile{Data: []byte(tc.value)}
			got, source := detect(files, tc.soft)
			if got != tc.want || source != tc.source {
				t.Fatalf("got %d %s, want %d %s", got, source, tc.want, tc.source)
			}
		})
	}
}
