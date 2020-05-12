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
	"sort"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// OperationGroup is a group of related operations
// If all operations in a group have the same operation.Type,
// the Type is also populated.
type OperationGroup struct {
	Type             string
	Operations       []*types.Operation
	Currencies       []*types.Currency
	NilAmountPresent bool
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
	// Remove group type if different
	if op.Type != destination.Type && destination.Type != "" {
		destination.Type = ""
	}

	// Update op assignment
	destination.Operations = append(destination.Operations, op)
	(*assignments)[op.OperationIdentifier.Index] = destinationIndex

	// Handle nil currency
	if op.Amount == nil {
		destination.NilAmountPresent = true
		return
	}

	// Add op to currency if amount is not nil
	if !asserter.ContainsCurrency(destination.Currencies, op.Amount.Currency) {
		destination.Currencies = append(destination.Currencies, op.Amount.Currency)
	}
}

func sortOperationGroups(opLen int, opGroups map[int]*OperationGroup) []*OperationGroup {
	sliceGroups := []*OperationGroup{}

	// Golang map ordering is non-deterministic.
	// Return groups sorted by lowest op in group
	for i := 0; i < opLen; i++ {
		v, ok := opGroups[i]
		if !ok {
			continue
		}

		// Sort all operations by index in a group
		sort.Slice(v.Operations, func(i, j int) bool {
			return v.Operations[i].OperationIdentifier.Index < v.Operations[j].OperationIdentifier.Index
		})

		sliceGroups = append(sliceGroups, v)
	}

	return sliceGroups
}

// GroupOperations parses all of a transaction's opertations and returns a slice
// of each group of related operations. This should ONLY be called on operations
// that have already been asserted for correctness. Assertion ensures there are
// no duplicate operation indexes, operations are sorted, and that operations
// only reference operations with an index less than theirs.
//
// OperationGroups are returned in ascending order based on the lowest
// operation index in the group.
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

			if op.Amount != nil {
				opGroups[key].Currencies = []*types.Currency{op.Amount.Currency}
			} else {
				opGroups[key].Currencies = []*types.Currency{}
				opGroups[key].NilAmountPresent = true
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

		// Ensure first index is lowest because all other groups
		// will be merged into it.
		sort.Ints(groupsToMerge)

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

	return sortOperationGroups(len(ops), opGroups)
}
