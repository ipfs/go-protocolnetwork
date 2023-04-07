package ipldbind

import (
	_ "embed"

	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
)

//go:embed schema.ipldsch
var embedSchema []byte

var BindnodeRegistry = bindnoderegistry.NewRegistry()

type Message struct {
	Id      []byte
	Payload []byte
}

func init() {
	if err := BindnodeRegistry.RegisterType((*Message)(nil), string(embedSchema), "Message"); err != nil {
		panic(err.Error())
	}
}
