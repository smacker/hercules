package internal_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/hercules.v8/internal/core"
	uast_items "gopkg.in/src-d/hercules.v8/internal/plumbing/uast"
	"gopkg.in/src-d/hercules.v8/internal/test"
	"gopkg.in/src-d/hercules.v8/leaves"
)

func TestPipelineSerialize(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	pipeline.SetFeature(uast_items.FeatureUast)
	pipeline.DeployItem(&leaves.BurndownAnalysis{})
	facts := map[string]interface{}{}
	facts[core.ConfigPipelineDryRun] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts[core.ConfigPipelineDAGPath] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	assert.Equal(t, `digraph Hercules {
  "6 BlobCache" -> "7 [blob_cache]"
  "0 DaysSinceStart" -> "3 [day]"
  "9 FileDiff" -> "11 [file_diff]"
  "15 FileDiffRefiner" -> "16 Burndown"
  "1 IdentityDetector" -> "4 [author]"
  "8 RenameAnalysis" -> "16 Burndown"
  "8 RenameAnalysis" -> "9 FileDiff"
  "8 RenameAnalysis" -> "10 UAST"
  "8 RenameAnalysis" -> "13 UASTChanges"
  "2 TreeDiff" -> "5 [changes]"
  "10 UAST" -> "12 [uasts]"
  "13 UASTChanges" -> "14 [changed_uasts]"
  "4 [author]" -> "16 Burndown"
  "7 [blob_cache]" -> "16 Burndown"
  "7 [blob_cache]" -> "9 FileDiff"
  "7 [blob_cache]" -> "8 RenameAnalysis"
  "7 [blob_cache]" -> "10 UAST"
  "14 [changed_uasts]" -> "15 FileDiffRefiner"
  "5 [changes]" -> "6 BlobCache"
  "5 [changes]" -> "8 RenameAnalysis"
  "3 [day]" -> "16 Burndown"
  "11 [file_diff]" -> "15 FileDiffRefiner"
  "12 [uasts]" -> "13 UASTChanges"
}`, dot)
}

func TestPipelineSerializeNoUast(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	// pipeline.SetFeature(FeatureUast)
	pipeline.DeployItem(&leaves.BurndownAnalysis{})
	facts := map[string]interface{}{}
	facts[core.ConfigPipelineDryRun] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts[core.ConfigPipelineDAGPath] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	assert.Equal(t, `digraph Hercules {
  "6 BlobCache" -> "7 [blob_cache]"
  "0 DaysSinceStart" -> "3 [day]"
  "9 FileDiff" -> "10 [file_diff]"
  "1 IdentityDetector" -> "4 [author]"
  "8 RenameAnalysis" -> "11 Burndown"
  "8 RenameAnalysis" -> "9 FileDiff"
  "2 TreeDiff" -> "5 [changes]"
  "4 [author]" -> "11 Burndown"
  "7 [blob_cache]" -> "11 Burndown"
  "7 [blob_cache]" -> "9 FileDiff"
  "7 [blob_cache]" -> "8 RenameAnalysis"
  "5 [changes]" -> "6 BlobCache"
  "5 [changes]" -> "8 RenameAnalysis"
  "3 [day]" -> "11 Burndown"
  "10 [file_diff]" -> "11 Burndown"
}`, dot)
}

func TestPipelineResolveIntegration(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	pipeline.DeployItem(&leaves.BurndownAnalysis{})
	pipeline.DeployItem(&leaves.CouplesAnalysis{})
	pipeline.Initialize(nil)
}
