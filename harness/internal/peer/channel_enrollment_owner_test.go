package peer

import "testing"

func TestChannelEnrollmentOwnerBudgetIsBoundedAndReusable(t *testing.T) {
	owner := &ChannelEnrollmentOwner{budget: make(chan struct{}, 1)}
	if !owner.acquireBudget() || owner.acquireBudget() {
		t.Fatal("owner enrollment budget did not enforce its exact capacity")
	}
	owner.releaseBudget()
	if !owner.acquireBudget() {
		t.Fatal("owner enrollment budget did not release its capacity")
	}
	owner.releaseBudget()
}
