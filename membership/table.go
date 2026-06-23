package membership

import (
	"clusterpulse/protocol"
	"sync"
	"time"
)

type Record struct {
	NodeID      string
	Status      protocol.MemberStatus
	Incarnation int64
	UpdatedAt   time.Time
}

type Table struct {
	mu         sync.RWMutex
	entries    map[string]Record
	lastUpdate time.Time
}

func NewTable(selfID string, knownNodes []string) *Table {
	t := &Table{
		entries: make(map[string]Record, len(knownNodes)+1),
	}

	now := time.Now()

	for _, nodeID := range knownNodes {
		if nodeID == "" {
			continue
		}
		if _, exists := t.entries[nodeID]; exists {
			continue
		}
		t.entries[nodeID] = Record{
			NodeID:      nodeID,
			Status:      protocol.StatusAlive,
			Incarnation: 0,
			UpdatedAt:   now,
		}
	}
	if _, exists := t.entries[selfID]; !exists {
		t.entries[selfID] = Record{
			NodeID:      selfID,
			Status:      protocol.StatusAlive,
			Incarnation: 0,
			UpdatedAt:   now,
		}
	}
	return t
}
func (t *Table) setStatus(nodeID string, status protocol.MemberStatus, observedAt time.Time) Record {
	t.mu.Lock()
	defer t.mu.Unlock()
	record, exists := t.entries[nodeID]
	if !exists {
		record = Record{NodeID: nodeID}
	}
	if exists && statusRank(status) < statusRank(record.Status) {
		return record
	}

	record.Status = status
	record.UpdatedAt = observedAt
	t.entries[nodeID] = record
	return record

}
func (t *Table) MarkAlive(nodeID string, observedAt time.Time) Record {
	return t.setStatus(nodeID, protocol.StatusAlive, observedAt)

}

func (t *Table) BumpIncarnation(nodeID string, observedAt time.Time) Record {
	t.mu.Lock()
	defer t.mu.Unlock()

	record, exists := t.entries[nodeID]
	if !exists {
		record = Record{NodeID: nodeID}
	}

	record.Incarnation++
	record.Status = protocol.StatusAlive
	record.UpdatedAt = observedAt
	t.entries[nodeID] = record

	return record
}

func (t *Table) MarkSuspect(nodeID string, observedAt time.Time) Record {
	return t.setStatus(nodeID, protocol.StatusSuspect, observedAt)
}
func (t *Table) MarkFailed(nodeID string, observedAt time.Time) Record {
	return t.setStatus(nodeID, protocol.StatusFailed, observedAt)
}
func (t *Table) MarkLeft(nodeID string, observedAt time.Time) Record {
	return t.setStatus(nodeID, protocol.StatusLeft, observedAt)
}

func (t *Table) Updates() []protocol.Update {
	t.mu.Lock()
	defer t.mu.Unlock()

	updates := make([]protocol.Update, 0, len(t.entries))
	cutoff := time.Now()
	for _, record := range t.entries {
		if lastUpdate := t.lastUpdate; !record.UpdatedAt.After(lastUpdate) {
			continue
		}
		updates = append(updates, protocol.Update{
			NodeID:      record.NodeID,
			Status:      record.Status,
			Incarnation: record.Incarnation,
			ObservedAt:  record.UpdatedAt,
		})
	}
	t.lastUpdate = cutoff
	return updates
}

func (t *Table) Get(nodeID string) (Record, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if record, exists := t.entries[nodeID]; exists {
		return record, true
	}
	return Record{}, false
}

func (t *Table) Snapshot() []Record {
	t.mu.RLock()
	defer t.mu.RUnlock()

	records := make([]Record, 0, len(t.entries))
	for _, record := range t.entries {
		records = append(records, record)
	}
	return records
}

func (t *Table) Merge(update protocol.Update) bool {
	if update.NodeID == "" {
		return false
	}
	status := update.Status
	incarnation := update.Incarnation
	observedAt := update.ObservedAt
	t.mu.Lock()
	defer t.mu.Unlock()
	record, exists := t.entries[update.NodeID]
	if !exists || record.Incarnation < incarnation {
		t.entries[update.NodeID] = Record{
			NodeID:      update.NodeID,
			Status:      status,
			Incarnation: incarnation,
			UpdatedAt:   observedAt,
		}
		return true
	}
	if incarnation == record.Incarnation {
		if statusRank(status) > statusRank(record.Status) {
			t.entries[update.NodeID] = Record{
				NodeID:      update.NodeID,
				Status:      status,
				Incarnation: incarnation,
				UpdatedAt:   observedAt,
			}
			return true
		}

	}
	return false
}
func statusRank(status protocol.MemberStatus) int {
	switch status {
	case protocol.StatusAlive:
		return 1
	case protocol.StatusSuspect:
		return 2
	case protocol.StatusFailed:
		return 3
	case protocol.StatusLeft:
		return 4
	default:
		return 0
	}
}
