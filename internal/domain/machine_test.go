package domain

import "testing"

func TestStateMachineHappyPath(t *testing.T) {
	path := []string{
		StatusPending, StatusAssigned, StatusAccepted, StatusEnRoute, StatusArrived,
		StatusResidentVerified, StatusWeighed, StatusConfirmed, StatusSettled,
		StatusDelivered, StatusInboundConfirmed, StatusCompleted,
	}
	for i := 0; i+1 < len(path); i++ {
		if err := MustTransit(path[i], path[i+1]); err != nil {
			t.Fatalf("%s -> %s should be legal: %v", path[i], path[i+1], err)
		}
	}
}

func TestStateMachineIllegal(t *testing.T) {
	cases := [][2]string{
		{StatusPending, StatusWeighed},     // 跳级
		{StatusPending, StatusSettled},     // 跳级
		{StatusCompleted, StatusCancelled}, // 终态不可取消
		{StatusCancelled, StatusAssigned},  // 取消后不可复活
		{StatusWeighed, StatusSettled},     // 未确认不可结算
		{StatusConfirmed, StatusWeighed},   // 确认后不可回退称重
		{StatusDelivered, StatusSettled},   // 不可逆
		{StatusArrived, StatusWeighed},     // 未核验居民不可称重
	}
	for _, c := range cases {
		if err := MustTransit(c[0], c[1]); err == nil {
			t.Fatalf("%s -> %s should be illegal", c[0], c[1])
		}
	}
}

func TestDisputeFlow(t *testing.T) {
	// 称重 -> 争议 -> (裁决) 回到称重 -> 确认
	if err := MustTransit(StatusWeighed, StatusDisputed); err != nil {
		t.Fatal(err)
	}
	if err := MustTransit(StatusDisputed, StatusWeighed); err != nil {
		t.Fatal(err)
	}
	if err := MustTransit(StatusDisputed, StatusConfirmed); err != nil {
		t.Fatal(err)
	}
}

func TestCancellable(t *testing.T) {
	for _, s := range []string{StatusPending, StatusAssigned, StatusAccepted, StatusEnRoute, StatusArrived} {
		if !CancellableByResident(s) {
			t.Fatalf("%s should be cancellable", s)
		}
	}
	for _, s := range []string{StatusWeighed, StatusConfirmed, StatusSettled, StatusCompleted} {
		if CancellableByResident(s) {
			t.Fatalf("%s should not be cancellable", s)
		}
	}
}
