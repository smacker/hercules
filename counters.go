package hercules

// FIXME matrix code and groupByDay (maybe) is way too similar

// globalCounter is the current daily alive number of lines
type globalCounter struct {
	// commit_day[day]value
	diffs   map[int]map[int]int64
	lastDay int
}

func newGlobalCounter() *globalCounter {
	return &globalCounter{diffs: make(map[int]map[int]int64)}
}

func (c *globalCounter) update(commitDay, updateDay int, delta int64) {
	if _, ok := c.diffs[commitDay]; !ok {
		c.diffs[commitDay] = make(map[int]int64)
	}
	c.diffs[commitDay][updateDay] += delta
	if commitDay > c.lastDay {
		c.lastDay = commitDay
	}
}

func (c *globalCounter) matrix(sampling, granularity int) [][]int64 {
	result := make([][]int64, 0)

	previousDay := 0
	for day := 0; day <= c.lastDay; day++ {
		delta := (day / sampling) - (previousDay / sampling)
		if delta > 0 {
			status := c.groupByDay(granularity, day)
			for i := 0; i < delta; i++ {
				result = append(result, status)
			}
			previousDay = day
		}
	}
	// last day
	status := c.groupByDay(granularity, c.lastDay+1)
	result = append(result, status)

	return result
}

// calculate alive number of lines on specific day
func (c *globalCounter) groupByDay(granularity, day int) []int64 {
	if granularity == 0 {
		granularity = 1
	}
	adjust := 0
	if day%granularity != 0 {
		adjust = 1
	}
	status := make([]int64, day/granularity+adjust)
	var group int64
	for i := 0; i < day; i++ {
		for j := 0; j < day; j++ {
			group += c.diffs[j][i]
		}

		if (i % granularity) == (granularity - 1) {
			status[i/granularity] = group
			group = 0
		}
	}
	if day%granularity != 0 {
		status[len(status)-1] = group
	}

	return status
}

type peopleCounter struct {
	// commit_day[][day]value
	diffs   []map[int]map[int]int64
	lastDay int
}

func newPeopleCounter(n int) *peopleCounter {
	return &peopleCounter{
		diffs: make([]map[int]map[int]int64, n),
	}
}

func (c *peopleCounter) update(commitDay, author, updateDay int, delta int64) {
	diffs := c.diffs[author]
	if diffs == nil {
		diffs = make(map[int]map[int]int64)
		c.diffs[author] = diffs
	}

	if _, ok := diffs[commitDay]; !ok {
		diffs[commitDay] = make(map[int]int64)
	}
	diffs[commitDay][updateDay] += delta

	if commitDay > c.lastDay {
		c.lastDay = commitDay
	}
}

// FIXME optimize
func (c *peopleCounter) matrix(sampling, granularity int) [][][]int64 {
	result := make([][][]int64, len(c.diffs))

	previousDay := 0
	for day := 0; day <= c.lastDay; day++ {
		delta := (day / sampling) - (previousDay / sampling)
		if delta > 0 {
			status := c.groupByDay(granularity, day)
			for key, ph := range status {
				for i := 0; i < delta; i++ {
					result[key] = append(result[key], ph)
				}
			}
			previousDay = day
		}
	}
	// last day
	status := c.groupByDay(granularity, c.lastDay+1)
	for key, ph := range status {
		result[key] = append(result[key], ph)
	}

	return result
}

// calculate alive number of lines on specific day
func (c *peopleCounter) groupByDay(granularity, day int) [][]int64 {
	if granularity == 0 {
		granularity = 1
	}
	adjust := 0
	if day%granularity != 0 {
		adjust = 1
	}

	peoples := make([][]int64, len(c.diffs))
	for key, person := range c.diffs {
		status := make([]int64, day/granularity+adjust)
		var group int64
		for i := 0; i < day; i++ {
			for j := 0; j < day; j++ {
				group += person[j][i]
			}
			if (i % granularity) == (granularity - 1) {
				status[i/granularity] = group
				group = 0
			}
		}
		if day%granularity != 0 {
			status[len(status)-1] = group
		}
		peoples[key] = status
	}

	return peoples
}
