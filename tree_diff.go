package hercules

import (
	"io"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// TreeDiff generates the list of changes for a commit. A change can be either one or two blobs
// under the same path: "before" and "after". If "before" is nil, the change is an addition.
// If "after" is nil, the change is a removal. Otherwise, it is a modification.
// TreeDiff is a PipelineItem.
type TreeDiff struct {
	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository
}

const (
	// DependencyTreeChanges is the name of the dependency provided by TreeDiff.
	DependencyTreeChanges = "changes"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (treediff *TreeDiff) Name() string {
	return "TreeDiff"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by hercules.Registry to build the global map of providers.
func (treediff *TreeDiff) Provides() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (treediff *TreeDiff) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (treediff *TreeDiff) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (treediff *TreeDiff) Configure(facts map[string]interface{}) {}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (treediff *TreeDiff) Initialize(repository *git.Repository) {
	treediff.repository = repository
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, "commit" is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (treediff *TreeDiff) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}

	var diff object.Changes
	switch len(commit.ParentHashes) {
	case 0:
		diff = []*object.Change{}
		err = func() error {
			fileIter := tree.Files()
			defer fileIter.Close()
			for {
				file, err := fileIter.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				diff = append(diff, &object.Change{
					To: object.ChangeEntry{Name: file.Name, Tree: tree, TreeEntry: object.TreeEntry{
						Name: file.Name, Mode: file.Mode, Hash: file.Hash}}})
			}
			return nil
		}()
	case 1:
		parent, err := treediff.repository.CommitObject(commit.ParentHashes[0])
		if err != nil {
			return nil, err
		}
		parentTree, err := parent.Tree()
		if err != nil {
			return nil, err
		}
		diff, err = object.DiffTree(parentTree, tree)
		if err != nil {
			return nil, err
		}
	case 2:
		parent, err := treediff.repository.CommitObject(commit.ParentHashes[0])
		if err != nil {
			return nil, err
		}
		parentTree, err := parent.Tree()
		if err != nil {
			return nil, err
		}
		diff, err = object.DiffTree(parentTree, tree)
		if err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{DependencyTreeChanges: diff}, nil
}

func init() {
	Registry.Register(&TreeDiff{})
}
