package dcpcassandra

import (
	"go-dcp-cassandra/cassandra"
)

type DcpEventHandler struct {
	bulk     *cassandra.Bulk
	isFinite bool
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
	if h.isFinite {
		return
	}
	h.bulk.PrepareStartRebalancing()
}

func (h *DcpEventHandler) AfterStreamStop() {
}
