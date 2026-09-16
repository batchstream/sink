# Backend environment qualification

Test the database image, host kernel, CPU architecture and allocator configuration
together. Docker containers share the Docker VM or Linux host kernel. A passing
startup probe does not establish stability under sustained load.

## MongoDB and Linux rseq

The 2026-09-17 qualification reproduced MongoDB 8.2.12 ARM64 crashes on Docker
Desktop's `7.0.12-linuxkit` kernel using the pinned `mongo:8.2` image. One run
aborted with `std::length_error` and an impossible allocation size during Count;
a separate write-only run segfaulted during an ordinary upsert. Neither affected
sample passed reconciliation. These are backend process failures, and reducing
Sink concurrency alone is not a verified remedy.

The symptoms are consistent with the upstream [TCMalloc rseq kernel regression](https://github.com/google/tcmalloc/issues/292)
and [MongoDB kernel compatibility tracking](https://jira.mongodb.org/browse/SERVER-121912).
MongoDB's own [compatibility test](https://github.com/mongodb/mongo/blob/r8.3.8/jstests/noPassthrough/rseq_linux_compatibility/rseq_kernel_compatibility_check.js)
uses `GLIBC_TUNABLES=glibc.pthread.rseq=1` for the configuration with TCMalloc's
per-CPU cache disabled. The Sink quickstart, storage integration runner, fixed
resource benchmark and production suite use that setting in disposable MongoDB
containers:

```yaml
environment:
  GLIBC_TUNABLES: glibc.pthread.rseq=1
```

Apply it to MongoDB processes, not Sink. It is a test-environment compatibility
choice, not a general production tuning recommendation. Prefer a database/kernel
combination supported by the database vendor; verify vendor fixes and distribution
backports rather than assuming a kernel version string proves compatibility.
Changing this setting can change database CPU and memory behavior, so repeat
capacity measurements under identical settings for baseline and candidate.

Treat backend exit, OOM kill, fatal log, failed setup or failed reconciliation as
a failed qualification even if an earlier cell had zero errors. Preserve logs,
container state and image digest. Confirm the backend remains running through
reconciliation and repeat the affected workloads after changing the environment.

A database process failure can make write outcomes unknown. Sink must not report
uncommitted writes as successful, and callers must reconcile unknown outcomes
before replaying non-idempotent operations. The separate three-member MongoDB
qualification exercises primary election and majority loss; a single-node
benchmark does not establish those guarantees.
