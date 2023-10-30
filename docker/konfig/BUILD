load("//bazel/docker:compose.bzl", "docker_compose")

filegroup(
    name = "data_deps",
    srcs = [
        "init.sh",
        "mongodb.yml",
    ],
    visibility = ["//visibility:public"],
)

docker_compose(
    name = "docker_compose",
    clean_restart = True,
    compose_yml = ":docker-compose.yml",
    data = [
        ":data_deps",
    ],
    images = [
        "//go/konfig/src/main/app:konfig",
        "//go/konfig/src/main/proxy:konfig_proxy",
        "//docker/mongodb:6",
    ],
    project = "konfig",
)
