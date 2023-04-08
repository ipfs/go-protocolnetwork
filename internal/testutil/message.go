package testutil

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-protocolnetwork/internal/testutil/ipldbind"
	message_pb "github.com/ipfs/go-protocolnetwork/internal/testutil/pb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

type Message struct {
	Id      []byte
	Payload []byte
}

func (m *Message) SendTimeout() time.Duration {
	return 10 * time.Second
}

func (m *Message) Log(logger *logging.ZapEventLogger, eventName string) {
}

func (m *Message) Clone() *Message {
	return &Message{
		Id:      m.Id,
		Payload: m.Payload,
	}
}

type ProtoMessageHandler struct{}

func (mh *ProtoMessageHandler) FromNet(p peer.ID, r io.Reader) (*Message, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(p, reader)
}

func (mh *ProtoMessageHandler) FromMsgReader(p peer.ID, r msgio.Reader) (*Message, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return nil, err
	}

	var pb message_pb.Message
	err = proto.Unmarshal(msg, &pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return nil, err
	}

	return mh.fromProto(&pb)
}

func (mh *ProtoMessageHandler) fromProto(pb *message_pb.Message) (*Message, error) {
	return &Message{
		Id:      pb.Id,
		Payload: pb.Payload,
	}, nil
}

// ToNet writes a GraphSyncMessage in its v1.0.0 protobuf format to a writer
func (mh *ProtoMessageHandler) ToNet(p peer.ID, outgoing *Message, w io.Writer) error {
	msg, err := mh.ToProto(p, outgoing)
	if err != nil {
		return err
	}
	size := proto.Size(msg)
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], msg)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

func (mh *ProtoMessageHandler) ToProto(p peer.ID, outgoing *Message) (*message_pb.Message, error) {
	return &message_pb.Message{
		Id:      outgoing.Id,
		Payload: outgoing.Payload,
	}, nil
}

type IPLDMessageHandler struct{}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func (mh *IPLDMessageHandler) FromNet(p peer.ID, r io.Reader) (*Message, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(p, reader)
}

// FromMsgReader can deserialize a DAG-CBOR message into a GraphySyncMessage
func (mh *IPLDMessageHandler) FromMsgReader(_ peer.ID, r msgio.Reader) (*Message, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return nil, err
	}

	ipldMessage, err := ipldbind.BindnodeRegistry.TypeFromBytes(msg, (*ipldbind.Message)(nil), dagcbor.Decode)
	if err != nil {
		return nil, err
	}
	return mh.fromIPLD(ipldMessage.(*ipldbind.Message))
}

// ToProto converts a GraphSyncMessage to its ipldbind.GraphSyncMessageRoot equivalent
func (mh *IPLDMessageHandler) toIPLD(outgoing *Message) (*ipldbind.Message, error) {
	return &ipldbind.Message{
		Id:      outgoing.Id,
		Payload: outgoing.Payload,
	}, nil
}

// ToNet writes a GraphSyncMessage in its DAG-CBOR format to a writer,
// prefixed with a length uvar
func (mh *IPLDMessageHandler) ToNet(_ peer.ID, outgoing *Message, w io.Writer) error {
	msg, err := mh.toIPLD(outgoing)
	if err != nil {
		return err
	}

	lbuf := make([]byte, binary.MaxVarintLen64)
	buf := new(bytes.Buffer)
	buf.Write(lbuf)

	node := ipldbind.BindnodeRegistry.TypeToNode(msg)
	err = ipld.EncodeStreaming(buf, node.Representation(), dagcbor.Encode)
	if err != nil {
		return err
	}

	lbuflen := binary.PutUvarint(lbuf, uint64(buf.Len()-binary.MaxVarintLen64))
	out := buf.Bytes()
	copy(out[binary.MaxVarintLen64-lbuflen:], lbuf[:lbuflen])
	_, err = w.Write(out[binary.MaxVarintLen64-lbuflen:])

	return err
}

// Mapping from a ipldbind.GraphSyncMessageRoot object to a GraphSyncMessage object
func (mh *IPLDMessageHandler) fromIPLD(ibm *ipldbind.Message) (*Message, error) {
	return &Message{
		Id:      ibm.Id,
		Payload: ibm.Payload,
	}, nil
}

const ProtocolMockV1 = protocol.ID("/mock/v1")
const ProtocolMockV2 = protocol.ID("/mock/v2")

var DefaultProtocols = []protocol.ID{ProtocolMockV2, ProtocolMockV1}
