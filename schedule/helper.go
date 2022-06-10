package schedule

import "strings"

// checks if schedule id and epoch are valid
func CheckSchedule(s Schedule) error {
	if strings.TrimSpace(s.ID()) == "" {
		return ErrInvalidScheduleID
	}
	if s.Epoch() < 0 {
		return ErrInvalidScheduleEpoch
	}
	return nil
}

// checks if two slices of schedules are the same (order doesn't matter)
func AreSame(arr1, arr2 []Schedule) bool {
	if len(arr1) != len(arr2) {
		return false
	}
	counter := make(map[string]int)
	for _, s := range arr1 {
		counter[s.ID()]++
	}
	for _, s := range arr2 {
		if _, found := counter[s.ID()]; !found {
			return false
		}
		counter[s.ID()]--
		if counter[s.ID()] == 0 {
			delete(counter, s.ID())
		}
	}
	return len(counter) == 0
}
