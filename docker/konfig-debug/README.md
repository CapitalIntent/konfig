# konfig-debug

Image variant of `konfig` that ships a busybox shell + common POSIX applets
for in-cluster triage. Same `konfig` binary as the production image, layered
onto `gcr.io/distroless/cc-debian13:debug-nonroot` instead of the slim
`:nonroot` base.

## Why

The production `konfig` image is distroless: no shell, no `cat`, no `ps`,
no `wget`. That keeps the attack surface small but every diagnostic session
needed `kubectl debug --image=...` to attach an ephemeral container, which
added ~30s of overhead per session and required the cluster operator to know
the netshoot/busybox image off the top of their head.

`konfig-debug` puts the diagnostic tools directly into the running pod's
filesystem so `kubectl exec` Just Works.

## Build / push

```sh
# Load locally (loads the host-arch image into the local docker daemon).
bazel run //docker/konfig-debug:load

# Push the multi-arch index (amd64 + arm64) to Docker Hub.
bazel run //docker/konfig-debug:push -- --tag latest
```

## Use

The image runs the same `/konfig` entrypoint as the production image, so it
is drop-in compatible with the production Deployment — change the image tag
only:

```yaml
# Deployment.spec.template.spec.containers[0]
image: kasa288/konfig-debug:latest
```

`kubectl exec` into a running debug pod for triage:

```sh
kubectl exec -it deploy/konfig -- /bin/sh
# inside the pod:
/ # ps
/ # cat /proc/1/status
/ # wget -qO- http://localhost:9090/metrics | head
/ # nslookup konfig-leader-elect
```

For ad-hoc debug Pods that should *not* start konfig at all (e.g. to poke at
a sibling service from the same network namespace), override the command in
the PodSpec:

```yaml
# Pod.spec.containers[0]
image: kasa288/konfig-debug:latest
command: ["/bin/sh"]
args: ["-c", "sleep infinity"]
```

## What's inside

| Path              | Source                                        |
|-------------------|-----------------------------------------------|
| `/konfig`         | konfig server binary (same as prod image)     |
| `/busybox/`       | busybox + applets (sh, ps, cat, ls, wget, ...)|
| `/usr/bin/sh`     | symlink → `/busybox/sh` so `/bin/sh` works    |
| `/usr/bin/cat`    | symlink → `/busybox/cat`                      |
| `/usr/bin/ls`     | symlink → `/busybox/ls`                       |
| `/usr/bin/ps`     | symlink → `/busybox/ps`                       |
| `/usr/bin/wget`   | symlink → `/busybox/wget`                     |
| `/usr/bin/nslookup` | symlink → `/busybox/nslookup`               |
| `/usr/bin/netstat` | symlink → `/busybox/netstat`                 |

Full applet list available at `/busybox/` — the symlinks just expose the
most commonly used ones at the conventional `/bin` paths.

## Size budget

Image content size is ~24 MB (well under the 50 MB budget), only ~1 MB
larger than the production `konfig` image (the delta is busybox itself).
The shared `konfig_bin` layer is content-addressed so an operator who
already has `kasa288/konfig` pulled only pays the busybox layer on first
pull of `konfig-debug`.

## Not bundled

`tcpdump` is *not* in busybox. For packet capture, fall back to
`kubectl debug --image=nicolaka/netshoot` for that one session, or open a
follow-up ticket to evaluate alpine/netshoot as a richer debug base.
