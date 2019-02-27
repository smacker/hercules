package leaves

import (
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v8/internal/core"
	"gopkg.in/src-d/hercules.v8/internal/pb"
	items "gopkg.in/src-d/hercules.v8/internal/plumbing"
	"gopkg.in/src-d/hercules.v8/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v8/internal/yaml"
)

// CommitsAnalysis extracts statistics for each commit
type CommitsAnalysis struct {
	core.NoopMerger
	core.OneShotMergeProcessor

	// days maps days to developers to stats
	commits []*CommitStat
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// CommitsResult is returned by CommitsAnalysis.Finalize() and carries the statistics
// per commit.
type CommitsResult struct {
	Commits []*CommitStat

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// FileStat is a statistic for a file
type FileStat struct {
	ToName   string
	FromName string
	Language string
	LineStats
}

// CommitStat is the statistics for a commit
type CommitStat struct {
	Hash   string
	When   int64
	Author int
	Files  []FileStat
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ca *CommitsAnalysis) Name() string {
	return "CommitsStat"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ca *CommitsAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ca *CommitsAnalysis) Requires() []string {
	arr := [...]string{
		identity.DependencyAuthor, items.DependencyTreeChanges, items.DependencyFileDiff,
		items.DependencyBlobCache, items.DependencyLanguages}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ca *CommitsAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return nil
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ca *CommitsAnalysis) Configure(facts map[string]interface{}) error {
	if val, exists := facts[identity.FactIdentityDetectorReversedPeopleDict].([]string); exists {
		ca.reversedPeopleDict = val
	}
	return nil
}

// Flag for the command line switch which enables this analysis.
func (ca *CommitsAnalysis) Flag() string {
	return "commits-stat"
}

// Description returns the text which explains what the analysis is doing.
func (ca *CommitsAnalysis) Description() string {
	return "Extracts statistics for each commit."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ca *CommitsAnalysis) Initialize(repository *git.Repository) error {
	ca.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ca *CommitsAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !ca.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	if deps[core.DependencyIsMerge].(bool) {
		return nil, nil
	}

	commit := deps[core.DependencyCommit].(*object.Commit)
	author := deps[identity.DependencyAuthor].(int)
	treeDiff := deps[items.DependencyTreeChanges].(object.Changes)
	if len(treeDiff) == 0 {
		return nil, nil
	}

	cs := CommitStat{
		Hash:   commit.Hash.String(),
		When:   commit.Author.When.Unix(),
		Author: author,
	}

	filesMap := make(map[string]*FileStat)
	cache := deps[items.DependencyBlobCache].(map[plumbing.Hash]*items.CachedBlob)
	fileDiffs := deps[items.DependencyFileDiff].(map[string]items.FileDiffData)
	langs := deps[items.DependencyLanguages].(map[plumbing.Hash]string)
	for _, change := range treeDiff {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}

		cf, ok := filesMap[change.To.Name+change.From.Name]
		if !ok {
			cf = &FileStat{}
			filesMap[change.To.Name+change.From.Name] = cf
		}
		cf.ToName = change.To.Name
		cf.FromName = change.From.Name

		switch action {
		case merkletrie.Insert:
			blob := cache[change.To.TreeEntry.Hash]
			lines, err := blob.CountLines()
			if err != nil {
				// binary
				continue
			}
			cf.Added += lines
			cf.Language = langs[change.To.TreeEntry.Hash]
		case merkletrie.Delete:
			blob := cache[change.From.TreeEntry.Hash]
			lines, err := blob.CountLines()
			if err != nil {
				// binary
				continue
			}
			cf.Removed += lines
			cf.Language = langs[change.From.TreeEntry.Hash]
		case merkletrie.Modify:
			cf.Language = langs[change.To.TreeEntry.Hash]
			thisDiffs := fileDiffs[change.To.Name]
			var removedPending int
			for _, edit := range thisDiffs.Diffs {
				switch edit.Type {
				case diffmatchpatch.DiffEqual:
					if removedPending > 0 {
						cf.Removed += removedPending
					}
					removedPending = 0
				case diffmatchpatch.DiffInsert:
					added := utf8.RuneCountInString(edit.Text)
					if removedPending > added {
						removed := removedPending - added
						cf.Changed += added
						cf.Removed += removed

					} else {
						added := added - removedPending
						cf.Changed += removedPending
						cf.Added += added

					}
					removedPending = 0
				case diffmatchpatch.DiffDelete:
					removedPending = utf8.RuneCountInString(edit.Text)
				}
			}
			if removedPending > 0 {
				cf.Removed += removedPending
			}
		}
	}

	for _, f := range filesMap {
		cs.Files = append(cs.Files, *f)
	}

	ca.commits = append(ca.commits, &cs)

	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (ca *CommitsAnalysis) Finalize() interface{} {
	return CommitsResult{
		Commits:            ca.commits,
		reversedPeopleDict: ca.reversedPeopleDict,
	}
}

// Fork clones this pipeline item.
func (ca *CommitsAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(ca, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (ca *CommitsAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	commitsResult := result.(CommitsResult)
	if binary {
		return ca.serializeBinary(&commitsResult, writer)
	}
	ca.serializeText(&commitsResult, writer)
	return nil
}

// Deserialize converts the specified protobuf bytes to DevsResult.
func (ca *CommitsAnalysis) Deserialize(pbmessage []byte) (interface{}, error) {
	panic("not implemented")
}

// MergeResults combines two DevsAnalysis-es together.
func (ca *CommitsAnalysis) MergeResults(r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	panic("not implemented")
}

func (ca *CommitsAnalysis) serializeText(result *CommitsResult, writer io.Writer) {
	fmt.Fprintln(writer, "  commits:")
	for _, c := range result.Commits {
		fmt.Fprintf(writer, "    - hash: %s\n", c.Hash)
		fmt.Fprintf(writer, "      when: %d\n", c.When)
		fmt.Fprintf(writer, "      author: %d\n", c.Author)
		fmt.Fprintf(writer, "      files:\n")
		for _, f := range c.Files {
			fmt.Fprintf(writer, "       - to: %s\n", f.ToName)
			fmt.Fprintf(writer, "         from: %s\n", f.FromName)
			fmt.Fprintf(writer, "         language: %s\n", f.Language)
			fmt.Fprintf(writer, "         stat: [%d, %d, %d]\n", f.Added, f.Changed, f.Removed)
		}
	}
	fmt.Fprintln(writer, "  people:")
	for _, person := range result.reversedPeopleDict {
		fmt.Fprintf(writer, "  - %s\n", yaml.SafeString(person))
	}
}

func (ca *CommitsAnalysis) serializeBinary(result *CommitsResult, writer io.Writer) error {
	message := pb.CommitsAnalysisResults{}
	message.AuthorIndex = result.reversedPeopleDict
	message.Commits = make([]*pb.Commit, len(result.Commits))
	for i, c := range result.Commits {
		files := make([]*pb.CommitFile, len(c.Files))
		for i, f := range c.Files {
			files[i] = &pb.CommitFile{
				To:       f.ToName,
				From:     f.FromName,
				Language: f.Language,
				Stats: &pb.LineStats{
					Added:   int32(f.LineStats.Added),
					Changed: int32(f.LineStats.Changed),
					Removed: int32(f.LineStats.Removed),
				},
			}
		}

		message.Commits[i] = &pb.Commit{
			Hash:         c.Hash,
			WhenUnixTime: c.When,
			Author:       int32(c.Author),
			Files:        files,
		}
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

func init() {
	core.Registry.Register(&CommitsAnalysis{})
}
