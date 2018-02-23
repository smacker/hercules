package hercules

import (
	"fmt"
	"sync"
)

// MergeResults combines two BurndownResult-s together.
func (analyser *BurndownAnalysis) MergeResults(
	r1, r2 interface{}, c1, c2 *CommonAnalysisResult) interface{} {
	bar1 := r1.(BurndownResult)
	bar2 := r2.(BurndownResult)
	merged := BurndownResult{}
	if bar1.sampling < bar2.sampling {
		merged.sampling = bar1.sampling
	} else {
		merged.sampling = bar2.sampling
	}
	if bar1.granularity < bar2.granularity {
		merged.granularity = bar1.granularity
	} else {
		merged.granularity = bar2.granularity
	}
	var people map[string][3]int
	people, merged.reversedPeopleDict = IdentityDetector{}.MergeReversedDicts(
		bar1.reversedPeopleDict, bar2.reversedPeopleDict)
	var wg sync.WaitGroup
	if len(bar1.GlobalHistory) > 0 || len(bar2.GlobalHistory) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			merged.GlobalHistory = mergeMatrices(
				bar1.GlobalHistory, bar2.GlobalHistory,
				bar1.granularity, bar1.sampling,
				bar2.granularity, bar2.sampling,
				c1, c2)
		}()
	}
	if len(bar1.FileHistories) > 0 || len(bar2.FileHistories) > 0 {
		merged.FileHistories = map[string][][]int64{}
		historyMutex := sync.Mutex{}
		for key, fh1 := range bar1.FileHistories {
			if fh2, exists := bar2.FileHistories[key]; exists {
				wg.Add(1)
				go func(fh1, fh2 [][]int64, key string) {
					defer wg.Done()
					historyMutex.Lock()
					defer historyMutex.Unlock()
					merged.FileHistories[key] = mergeMatrices(
						fh1, fh2, bar1.granularity, bar1.sampling, bar2.granularity, bar2.sampling, c1, c2)
				}(fh1, fh2, key)
			} else {
				historyMutex.Lock()
				merged.FileHistories[key] = fh1
				historyMutex.Unlock()
			}
		}
		for key, fh2 := range bar2.FileHistories {
			if _, exists := bar1.FileHistories[key]; !exists {
				historyMutex.Lock()
				merged.FileHistories[key] = fh2
				historyMutex.Unlock()
			}
		}
	}
	if len(merged.reversedPeopleDict) > 0 {
		merged.PeopleHistories = make([][][]int64, len(merged.reversedPeopleDict))
		for i, key := range merged.reversedPeopleDict {
			ptrs := people[key]
			if ptrs[1] < 0 {
				if len(bar2.PeopleHistories) > 0 {
					merged.PeopleHistories[i] = bar2.PeopleHistories[ptrs[2]]
				}
			} else if ptrs[2] < 0 {
				if len(bar1.PeopleHistories) > 0 {
					merged.PeopleHistories[i] = bar1.PeopleHistories[ptrs[1]]
				}
			} else {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					var m1, m2 [][]int64
					if len(bar1.PeopleHistories) > 0 {
						m1 = bar1.PeopleHistories[ptrs[1]]
					}
					if len(bar2.PeopleHistories) > 0 {
						m2 = bar2.PeopleHistories[ptrs[2]]
					}
					merged.PeopleHistories[i] = mergeMatrices(
						m1, m2,
						bar1.granularity, bar1.sampling,
						bar2.granularity, bar2.sampling,
						c1, c2,
					)
				}(i)
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if len(bar2.PeopleMatrix) == 0 {
				merged.PeopleMatrix = bar1.PeopleMatrix
				// extend the matrix in both directions
				for i := 0; i < len(merged.PeopleMatrix); i++ {
					for j := len(bar1.reversedPeopleDict); j < len(merged.reversedPeopleDict); j++ {
						merged.PeopleMatrix[i] = append(merged.PeopleMatrix[i], 0)
					}
				}
				for i := len(bar1.reversedPeopleDict); i < len(merged.reversedPeopleDict); i++ {
					merged.PeopleMatrix = append(
						merged.PeopleMatrix, make([]int64, len(merged.reversedPeopleDict)+2))
				}
			} else {
				merged.PeopleMatrix = make([][]int64, len(merged.reversedPeopleDict))
				for i := range merged.PeopleMatrix {
					merged.PeopleMatrix[i] = make([]int64, len(merged.reversedPeopleDict)+2)
				}
				for i, key := range bar1.reversedPeopleDict {
					mi := people[key][0] // index in merged.reversedPeopleDict
					copy(merged.PeopleMatrix[mi][:2], bar1.PeopleMatrix[i][:2])
					for j, val := range bar1.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar1.reversedPeopleDict[j]][0]] = val
					}
				}
				for i, key := range bar2.reversedPeopleDict {
					mi := people[key][0] // index in merged.reversedPeopleDict
					merged.PeopleMatrix[mi][0] += bar2.PeopleMatrix[i][0]
					merged.PeopleMatrix[mi][1] += bar2.PeopleMatrix[i][1]
					for j, val := range bar2.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar2.reversedPeopleDict[j]][0]] += val
					}
				}
			}
		}()
	}
	wg.Wait()
	return merged
}

// mergeMatrices takes two [number of samples][number of bands] matrices,
// resamples them to days so that they become square, sums and resamples back to the
// least of (sampling1, sampling2) and (granularity1, granularity2).
func mergeMatrices(m1, m2 [][]int64, granularity1, sampling1, granularity2, sampling2 int,
	c1, c2 *CommonAnalysisResult) [][]int64 {
	commonMerged := *c1
	commonMerged.Merge(c2)

	var granularity, sampling int
	if sampling1 < sampling2 {
		sampling = sampling1
	} else {
		sampling = sampling2
	}
	if granularity1 < granularity2 {
		granularity = granularity1
	} else {
		granularity = granularity2
	}

	size := int((commonMerged.EndTime - commonMerged.BeginTime) / (3600 * 24))
	daily := make([][]float32, size+granularity)
	for i := range daily {
		daily[i] = make([]float32, size+sampling)
	}
	if len(m1) > 0 {
		addBurndownMatrix(m1, granularity1, sampling1, daily,
			int(c1.BeginTime-commonMerged.BeginTime)/(3600*24))
	}
	if len(m2) > 0 {
		addBurndownMatrix(m2, granularity2, sampling2, daily,
			int(c2.BeginTime-commonMerged.BeginTime)/(3600*24))
	}

	// convert daily to [][]in(t64
	result := make([][]int64, (size+sampling-1)/sampling)
	for i := range result {
		result[i] = make([]int64, (size+granularity-1)/granularity)
		sampledIndex := i * sampling
		if i == len(result)-1 {
			sampledIndex = size - 1
		}
		for j := 0; j < len(result[i]); j++ {
			accum := float32(0)
			for k := j * granularity; k < (j+1)*granularity && k < size; k++ {
				accum += daily[sampledIndex][k]
			}
			result[i][j] = int64(accum)
		}
	}
	return result
}

// Explode `matrix` so that it is daily sampled and has daily bands, shift by `offset` days
// and add to the accumulator. `daily` size is square and is guaranteed to fit `matrix` by
// the caller.
// Rows: *at least* len(matrix) * sampling + offset
// Columns: *at least* len(matrix[...]) * granularity + offset
// `matrix` can be sparse, so that the last columns which are equal to 0 are truncated.
func addBurndownMatrix(matrix [][]int64, granularity, sampling int, daily [][]float32, offset int) {
	// Determine the maximum number of bands; the actual one may be larger but we do not care
	maxCols := 0
	for _, row := range matrix {
		if maxCols < len(row) {
			maxCols = len(row)
		}
	}
	neededRows := len(matrix)*sampling + offset
	if len(daily) < neededRows {
		panic(fmt.Sprintf("merge bug: too few daily rows: required %d, have %d",
			neededRows, len(daily)))
	}
	if len(daily[0]) < maxCols {
		panic(fmt.Sprintf("merge bug: too few daily cols: required %d, have %d",
			maxCols, len(daily[0])))
	}
	for x := 0; x < maxCols; x++ {
		for y := 0; y < len(matrix); y++ {
			if x*granularity > (y+1)*sampling {
				// the future is zeros
				continue
			}
			decay := func(startIndex int, startVal float32) {
				if startVal == 0 {
					return
				}
				k := float32(matrix[y][x]) / startVal // <= 1
				scale := float32((y+1)*sampling - startIndex)
				for i := x * granularity; i < (x+1)*granularity; i++ {
					initial := daily[startIndex-1+offset][i+offset]
					for j := startIndex; j < (y+1)*sampling; j++ {
						daily[j+offset][i+offset] = initial * (1 + (k-1)*float32(j-startIndex+1)/scale)
					}
				}
			}
			raise := func(finishIndex int, finishVal float32) {
				var initial float32
				if y > 0 {
					initial = float32(matrix[y-1][x])
				}
				startIndex := y * sampling
				if startIndex < x*granularity {
					startIndex = x * granularity
				}
				if startIndex == finishIndex {
					return
				}
				avg := (finishVal - initial) / float32(finishIndex-startIndex)
				for j := y * sampling; j < finishIndex; j++ {
					for i := startIndex; i <= j; i++ {
						daily[j+offset][i+offset] = avg
					}
				}
				// copy [x*g..y*s)
				for j := y * sampling; j < finishIndex; j++ {
					for i := x * granularity; i < y*sampling; i++ {
						daily[j+offset][i+offset] = daily[j-1+offset][i+offset]
					}
				}
			}
			if (x+1)*granularity >= (y+1)*sampling {
				// x*granularity <= (y+1)*sampling
				// 1. x*granularity <= y*sampling
				//    y*sampling..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / y      -|
				//   /
				//  / x
				//
				// 2. x*granularity > y*sampling
				//    x*granularity..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / x      -|
				//   /
				//  / y
				if x*granularity <= y*sampling {
					raise((y+1)*sampling, float32(matrix[y][x]))
				} else if (y+1)*sampling > x*granularity {
					raise((y+1)*sampling, float32(matrix[y][x]))
					avg := float32(matrix[y][x]) / float32((y+1)*sampling-x*granularity)
					for j := x * granularity; j < (y+1)*sampling; j++ {
						for i := x * granularity; i <= j; i++ {
							daily[j+offset][i+offset] = avg
						}
					}
				}
			} else if (x+1)*granularity >= y*sampling {
				// y*sampling <= (x+1)*granularity < (y+1)sampling
				// y*sampling..(x+1)*granularity
				// (x+1)*granularity..(y+1)sampling
				//        x+1
				//         /\
				//        /  \
				//       /    \
				//      /    y+1
				//     /
				//    y
				v1 := float32(matrix[y-1][x])
				v2 := float32(matrix[y][x])
				var peak float32
				delta := float32((x+1)*granularity - y*sampling)
				var scale float32
				var previous float32
				if y > 0 && (y-1)*sampling >= x*granularity {
					// x*g <= (y-1)*s <= y*s <= (x+1)*g <= (y+1)*s
					//           |________|.......^
					if y > 1 {
						previous = float32(matrix[y-2][x])
					}
					scale = float32(sampling)
				} else {
					// (y-1)*s < x*g <= y*s <= (x+1)*g <= (y+1)*s
					//            |______|.......^
					if y == 0 {
						scale = float32(sampling)
					} else {
						scale = float32(y*sampling - x*granularity)
					}
				}
				peak = v1 + (v1-previous)/scale*delta
				if v2 > peak {
					// we need to adjust the peak, it may not be less than the decayed value
					if y < len(matrix)-1 {
						// y*s <= (x+1)*g <= (y+1)*s < (y+2)*s
						//           ^.........|_________|
						k := (v2 - float32(matrix[y+1][x])) / float32(sampling) // > 0
						peak = float32(matrix[y][x]) + k*float32((y+1)*sampling-(x+1)*granularity)
						// peak > v2 > v1
					} else {
						peak = v2
						// not enough data to interpolate; this is at least not restricted
					}
				}
				raise((x+1)*granularity, peak)
				decay((x+1)*granularity, peak)
			} else {
				// (x+1)*granularity < y*sampling
				// y*sampling..(y+1)sampling
				decay(y*sampling, float32(matrix[y-1][x]))
			}
		}
	}
}
