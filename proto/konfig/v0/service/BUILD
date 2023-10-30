load("@rules_proto//proto:defs.bzl", "proto_descriptor_set", "proto_library")
load("@rules_proto_grpc//grpc-gateway:defs.bzl", "gateway_grpc_library")

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
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@go_googleapis//google/api:annotations_proto",
    ],
)

proto_descriptor_set(
    name = "descriptor_set",
    deps = [
        ":proto",
        "@go_googleapis//google/api:annotations_proto",
    ],
)

gateway_grpc_library(
    name = "service",
    importpath = "capitalintent.com/konfig/protos/v0/service",
    options = {
        "@rules_proto_grpc//grpc-gateway:grpc_gateway_plugin": ["allow_delete_body=true"],
    },
    protos = [":proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/konfig/v0/common",
    ],
)
