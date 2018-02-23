package hercules

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	git "gopkg.in/src-d/go-git.v4"
	yaml "gopkg.in/yaml.v2"
)

type Case struct {
	Source string
	Result string
}

var cases = []Case{
	{
		Source: "fixtures/simple_1_day_1_author",
		Result: "fixtures/simple_1_day_1_author.yml",
	},
	{
		Source: "fixtures/simple_1_day_odd_even_author",
		Result: "fixtures/simple_1_day_odd_even_author.yml",
	},
	{
		Source: "fixtures/simple_each_day_1_author",
		Result: "fixtures/simple_each_day_1_author.yml",
	},
	{
		Source: "fixtures/simple_merge_1_day_odd_even_author",
		Result: "fixtures/simple_merge_1_day_odd_even_author.yml",
	},
	// merge conflicts work incorrect actually
	{
		Source: "fixtures/merge_conflict",
		Result: "fixtures/merge_conflict.yml",
	},
	{
		Source: "fixtures/merge_conflict_only_remove",
		Result: "fixtures/merge_conflict_only_remove.yml",
	},
}

type Expected struct {
	W struct {
		Granularity int
		Sampling    int
		Project     string
		People      map[string]string
	} `yaml:"Burndown"`
}

func TestIntegration(t *testing.T) {
	assert := assert.New(t)

	for _, c := range cases {
		var expected Expected
		b, err := ioutil.ReadFile(c.Result)
		assert.NoError(err)
		err = yaml.Unmarshal(b, &expected)
		assert.NoError(err)

		var expGlobalHistory [][]int64
		expPeopleHistories := make(map[string][][]int64)
		for _, line := range strings.Split(expected.W.Project, "\n") {
			line = strings.TrimSpace(line)

			var row []int64
			for _, v := range strings.Split(line, "  ") {
				i, _ := strconv.ParseInt(v, 10, 64)
				row = append(row, i)
			}
			expGlobalHistory = append(expGlobalHistory, row)
		}
		for name, matrix := range expected.W.People {
			var m [][]int64
			for _, line := range strings.Split(matrix, "\n") {
				line = strings.TrimSpace(line)

				var row []int64
				for _, v := range strings.Split(line, "  ") {
					i, _ := strconv.ParseInt(v, 10, 64)
					row = append(row, i)
				}
				m = append(m, row)
			}
			expPeopleHistories[name] = m
		}

		repo, err := git.PlainOpen(c.Source)
		assert.NoError(err, fmt.Sprintf("can't open %s repository", c.Source))

		pipeline := NewPipeline(repo)
		commits := pipeline.Commits()
		burndownItem := Registry.Summon("Burndown")[0]
		pipeline.DeployItem(burndownItem)
		facts := map[string]interface{}{
			"commits":                 commits,
			ConfigBurndownTrackPeople: true,
			ConfigBurndownGranularity: expected.W.Granularity,
			ConfigBurndownSampling:    expected.W.Sampling,
		}
		pipeline.Initialize(facts)
		results, err := pipeline.Run(commits)
		assert.NoError(err)

		var r BurndownResult
		for li, v := range results {
			if li == nil {
				continue
			}
			if li.Name() == "Burndown" {
				r = v.(BurndownResult)
			}
		}

		assert.Equal(expGlobalHistory, makeMatrix2(r.GlobalHistory), fmt.Sprintf("global: %s", c.Source))
		assert.Equal(expPeopleHistories, makePeopleHistories(r.PeopleHistories, r.reversedPeopleDict), fmt.Sprintf("people: %s", c.Source))
	}
}

func makeMatrix2(input [][]int64) [][]int64 {
	var maxCol int
	for _, row := range input {
		if len(row) > maxCol {
			maxCol = len(row)
		}
	}

	result := make([][]int64, len(input))
	for i, inputRow := range input {
		result[i] = make([]int64, maxCol)
		for j, v := range inputRow {
			result[i][j] = v
		}
	}

	return result
}

func makePeopleHistories(input [][][]int64, people []string) map[string][][]int64 {
	result := make(map[string][][]int64, len(input))
	for i, m := range input {
		result[people[i]] = makeMatrix2(m)
	}

	return result
}
