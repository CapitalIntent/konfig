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
    visibility = ["//proto/konfig:__subpackages__"],
    deps = [
        "//proto/common:proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

go_grpc_library(
    name = "common",
    importpath = "capitalintent.com/konfig/protos/v0/common",
    protos = [":proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/common"],
)
