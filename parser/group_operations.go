// Copyright 2020 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"github.com/coinbase/rosetta-sdk-go/types"
)

// OperationGroup is a group of related operations
// If all operations in a group have the same operation.Type,
// the Type is also populated.
type OperationGroup struct {
	Type       string
	Operations []*types.Operation
}

func containsInt(valid []int, value int) bool {
	for _, v := range valid {
		if v == value {
			return true
		}
	}

	return false
}

func addOperationToGroup(
	destination *OperationGroup,
	destinationIndex int,
	assignments *[]int,
	op *types.Operation,
) {
	if op.Type != destination.Type && destination.Type != "" {
		destination.Type = ""
	}
	destination.Operations = append(destination.Operations, op)
	(*assignments)[op.OperationIdentifier.Index] = destinationIndex
}

// GroupOperations parses all of a transaction's opertations and returns a slice
// of each group of related operations. This should ONLY be called on operations
// that have already been asserted for correctness. Assertion ensures there are
// no duplicate operation indexes, operations are sorted, and that operations
// only reference operations with an index less than theirs.
func GroupOperations(transaction *types.Transaction) []*OperationGroup {
	ops := transaction.Operations
	opGroups := map[int]*OperationGroup{} // using a map makes group merges much easier
	opAssignments := make([]int, len(ops))
	for i, op := range ops {
		// Create new group
		if len(op.RelatedOperations) == 0 {
			key := len(opGroups)
			opGroups[key] = &OperationGroup{
				Type:       op.Type,
				Operations: []*types.Operation{op},
			}
			opAssignments[i] = key
			continue
		}

		// Find groups to merge
		groupsToMerge := []int{}
		for _, relatedOp := range op.RelatedOperations {
			if !containsInt(groupsToMerge, opAssignments[relatedOp.Index]) {
				groupsToMerge = append(groupsToMerge, opAssignments[relatedOp.Index])
			}
		}

		mergedGroupIndex := groupsToMerge[0]
		mergedGroup := opGroups[mergedGroupIndex]

		// Add op to unified group
		addOperationToGroup(mergedGroup, mergedGroupIndex, &opAssignments, op)

		// Merge Groups
		for _, otherGroupIndex := range groupsToMerge[1:] {
			otherGroup := opGroups[otherGroupIndex]

			// Add otherGroup ops to mergedGroup
			for _, otherOp := range otherGroup.Operations {
				addOperationToGroup(mergedGroup, mergedGroupIndex, &opAssignments, otherOp)
			}

			// Delete otherGroup
			delete(opGroups, otherGroupIndex)
		}
	}

	return func() []*OperationGroup {
		sliceGroups := []*OperationGroup{}
		for _, v := range opGroups {
			sliceGroups = append(sliceGroups, v)
		}

		return sliceGroups
	}()
}
