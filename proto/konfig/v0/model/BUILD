load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc//go:defs.bzl", "go_grpc_library")

filegroup(
    name = "srcs",
    srcs = glob(["*.proto"]),
    visibility = ["//visibility:public"],
)

proto_library(
    name = "proto",
    srcs = glob(["*.proto"]),
    visibility = ["//visibility:public"],
    deps = [
        "//proto/konfig/v0/common:proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

go_grpc_library(
    name = "model",
    importpath = "capitalintent.com/konfig/protos/v0/model",
    protos = [":proto"],
    visibility = [
        "//go/konfig/src/main:__subpackages__",
        "//go/konfig/src/test:__subpackages__",
    ],
    deps = [
        "//proto/konfig/v0/common",
    ],
)
