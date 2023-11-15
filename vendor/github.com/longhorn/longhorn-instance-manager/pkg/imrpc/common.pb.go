// Code generated by protoc-gen-go. DO NOT EDIT.
// source: github.com/longhorn/longhorn-instance-manager/pkg/imrpc/common.proto

package imrpc

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type BackendStoreDriver int32

const (
	BackendStoreDriver_v1 BackendStoreDriver = 0
	BackendStoreDriver_v2 BackendStoreDriver = 1
)

var BackendStoreDriver_name = map[int32]string{
	0: "v1",
	1: "v2",
}

var BackendStoreDriver_value = map[string]int32{
	"v1": 0,
	"v2": 1,
}

func (x BackendStoreDriver) String() string {
	return proto.EnumName(BackendStoreDriver_name, int32(x))
}

func (BackendStoreDriver) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_8198dc9351c39e1c, []int{0}
}

func init() {
	proto.RegisterEnum("imrpc.BackendStoreDriver", BackendStoreDriver_name, BackendStoreDriver_value)
}

func init() {
	proto.RegisterFile("github.com/longhorn/longhorn-instance-manager/pkg/imrpc/common.proto", fileDescriptor_8198dc9351c39e1c)
}

var fileDescriptor_8198dc9351c39e1c = []byte{
	// 130 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x72, 0x49, 0xcf, 0x2c, 0xc9,
	0x28, 0x4d, 0xd2, 0x4b, 0xce, 0xcf, 0xd5, 0xcf, 0xc9, 0xcf, 0x4b, 0xcf, 0xc8, 0x2f, 0xca, 0x83,
	0x33, 0x74, 0x33, 0xf3, 0x8a, 0x4b, 0x12, 0xf3, 0x92, 0x53, 0x75, 0x73, 0x13, 0xf3, 0x12, 0xd3,
	0x53, 0x8b, 0xf4, 0x0b, 0xb2, 0xd3, 0xf5, 0x33, 0x73, 0x8b, 0x0a, 0x92, 0xf5, 0x93, 0xf3, 0x73,
	0x73, 0xf3, 0xf3, 0xf4, 0x0a, 0x8a, 0xf2, 0x4b, 0xf2, 0x85, 0x58, 0xc1, 0x62, 0x5a, 0x2a, 0x5c,
	0x42, 0x4e, 0x89, 0xc9, 0xd9, 0xa9, 0x79, 0x29, 0xc1, 0x25, 0xf9, 0x45, 0xa9, 0x2e, 0x45, 0x99,
	0x65, 0xa9, 0x45, 0x42, 0x6c, 0x5c, 0x4c, 0x65, 0x86, 0x02, 0x0c, 0x60, 0xda, 0x48, 0x80, 0x31,
	0x89, 0x0d, 0xac, 0xc7, 0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0x54, 0xa7, 0xbe, 0xfc, 0x7b, 0x00,
	0x00, 0x00,
}
