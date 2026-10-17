package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"testing"
)

// TestFSMRestoreSortsNode2Slot checks the post-condition of Restore:
// Node2Slot must hold ascending distinct slot lists because addSlots and
// removeSlots rely on sort.Search. The list used to be built in map iteration
// order (random) which broke that invariant after a restore.
func TestFSMRestoreSortsNode2Slot(t *testing.T) {
	slot2Node := make(map[uint32]string)
	for slot := uint32(0); slot < 100; slot++ {
		slot2Node[slot] = "node1"
	}
	data, err := json.Marshal(&FSMSnapshot{
		Slot2Node:    slot2Node,
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
	})
	if err != nil {
		t.Fatalf("marshal snapshot failed: %v", err)
	}

	fsm := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}
	err = fsm.Restore(io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	slots := fsm.Node2Slot["node1"]
	if len(slots) != 100 {
		t.Fatalf("expected 100 slots, got %d", len(slots))
	}
	if !slices.IsSorted(slots) {
		t.Fatal("Node2Slot is not sorted after restore")
	}

	// addSlots / removeSlots rely on sorted arrays, verify they still behave
	fsm.addSlots("node1", []uint32{150, 99}) // 99 already exists, 150 appended
	slots = fsm.Node2Slot["node1"]
	if len(slots) != 101 || slots[100] != 150 {
		t.Fatalf("addSlots after restore corrupted the slot list: %v", slots)
	}
	fsm.removeSlots("node1", []uint32{0})
	slots = fsm.Node2Slot["node1"]
	if len(slots) != 100 || slices.Contains(slots, 0) {
		t.Fatalf("removeSlots after restore corrupted the slot list: %v", slots)
	}
	if _, ok := fsm.Slot2Node[0]; ok {
		t.Fatal("Slot2Node still holds a removed slot")
	}
}
