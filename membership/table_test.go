package membership

import (
	"testing"
	"time"

	"clusterpulse/protocol"
)

func TestMergeIgnoresOlderIncarnation(t *testing.T) {
	table := NewTable("node-1", []string{"node-1", "node-2"})
	now := time.Now()

	if !table.Merge(protocol.Update{
		NodeID:      "node-2",
		Status:      protocol.StatusAlive,
		Incarnation: 1,
		ObservedAt:  now,
	}) {
		t.Fatal("expected newer incarnation to merge")
	}

	if table.Merge(protocol.Update{
		NodeID:      "node-2",
		Status:      protocol.StatusFailed,
		Incarnation: 0,
		ObservedAt:  now.Add(time.Second),
	}) {
		t.Fatal("expected older incarnation to be ignored")
	}

	record, exists := table.Get("node-2")
	if !exists {
		t.Fatal("expected node-2 record to exist")
	}
	if record.Status != protocol.StatusAlive || record.Incarnation != 1 {
		t.Fatalf("expected alive incarnation 1, got %s incarnation %d", record.Status, record.Incarnation)
	}
}

func TestMergeWorseStatusWinsAtSameIncarnation(t *testing.T) {
	table := NewTable("node-1", []string{"node-1", "node-2"})
	now := time.Now()

	if !table.Merge(protocol.Update{
		NodeID:      "node-2",
		Status:      protocol.StatusSuspect,
		Incarnation: 0,
		ObservedAt:  now,
	}) {
		t.Fatal("expected suspect update to merge")
	}

	if table.Merge(protocol.Update{
		NodeID:      "node-2",
		Status:      protocol.StatusAlive,
		Incarnation: 0,
		ObservedAt:  now.Add(time.Second),
	}) {
		t.Fatal("expected lower-ranked same-incarnation alive update to be ignored")
	}

	record, exists := table.Get("node-2")
	if !exists {
		t.Fatal("expected node-2 record to exist")
	}
	if record.Status != protocol.StatusSuspect || record.Incarnation != 0 {
		t.Fatalf("expected suspect incarnation 0, got %s incarnation %d", record.Status, record.Incarnation)
	}
}

func TestBumpIncarnationMarksNodeAlive(t *testing.T) {
	table := NewTable("node-1", []string{"node-1", "node-2"})
	now := time.Now()

	table.MarkFailed("node-1", now)
	record := table.BumpIncarnation("node-1", now.Add(time.Second))

	if record.Status != protocol.StatusAlive {
		t.Fatalf("expected bumped record to be alive, got %s", record.Status)
	}
	if record.Incarnation != 1 {
		t.Fatalf("expected incarnation 1, got %d", record.Incarnation)
	}
}
