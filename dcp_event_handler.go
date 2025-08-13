package dcpcassandra

import (
	"go-dcp-cassandra/cassandra"
)

type DcpEventHandler struct {
	bulk *cassandra.Bulk
}

func (h *DcpEventHandler) BeforeRebalanceStart() {
}

func (h *DcpEventHandler) AfterRebalanceStart() {
}

func (h *DcpEventHandler) BeforeRebalanceEnd() {
}

func (h *DcpEventHandler) AfterRebalanceEnd() {
}

func (h *DcpEventHandler) BeforeStreamStart() {
	h.bulk.PrepareEndRebalancing()
}

func (h *DcpEventHandler) AfterStreamStart() {
}

func (h *DcpEventHandler) BeforeStreamStop() {
	h.bulk.PrepareStartRebalancing()
}

func (h *DcpEventHandler) AfterStreamStop() {
}
