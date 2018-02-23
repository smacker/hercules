package hercules

import (
	"fmt"
	"io"
	"os"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
	"gopkg.in/src-d/hercules.v3/yaml"
)

// BurndownAnalysis allows to gather the line burndown statistics for a Git repository.
// It is a LeafPipelineItem.
// Reference: https://erikbern.com/2016/12/05/the-half-life-of-code.html
type BurndownAnalysis struct {
	// Granularity sets the size of each band - the number of days it spans.
	// Smaller values provide better resolution but require more work and eat more
	// memory. 30 days is usually enough.
	Granularity int
	// Sampling sets how detailed is the statistic - the size of the interval in
	// days between consecutive measurements. It may not be greater than Granularity. Try 15 or 30.
	Sampling int

	// TrackFiles enables or disables the fine-grained per-file burndown analysis.
	// It does not change the project level burndown results.
	TrackFiles bool

	// The number of developers for which to collect the burndown stats. 0 disables it.
	PeopleNumber int

	// Debug activates the debugging mode. Analyse() runs slower in this mode
	// but it accurately checks all the intermediate states for invariant
	// violations.
	Debug bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository
	// globalStatus is the current daily alive number of lines
	globalStatus *globalCounter
	// fileHistories is the periodic snapshots of each file's status.
	fileHistories map[string][][]int64
	// files is the mapping <commit-hash> -> <file path> -> *File.
	files map[string]map[string]*File
	// matrix is the mutual deletions and self insertions.
	matrix []map[int]int64
	// people is the people's individual time stats.
	//people []map[int]int64
	people *peopleCounter
	// previousDay is the day from the previous sample period -
	// different from DaysSinceStart.previousDay.
	// previousDay int
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	commitDay int
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *BurndownAnalysis) Name() string {
	return "Burndown"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by hercules.Registry to build the global map of providers.
func (analyser *BurndownAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *BurndownAnalysis) Requires() []string {
	arr := [...]string{
		DependencyFileDiff, DependencyTreeChanges, DependencyBlobCache,
		DependencyDay, DependencyAuthor}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *BurndownAnalysis) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *BurndownAnalysis) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigBurndownGranularity].(int); exists {
		analyser.Granularity = val
	}
	if val, exists := facts[ConfigBurndownSampling].(int); exists {
		analyser.Sampling = val
	}
	if val, exists := facts[ConfigBurndownTrackFiles].(bool); exists {
		analyser.TrackFiles = val
	}
	if people, exists := facts[ConfigBurndownTrackPeople].(bool); people {
		if val, exists := facts[FactIdentityDetectorPeopleCount].(int); exists {
			analyser.PeopleNumber = val
			analyser.reversedPeopleDict = facts[FactIdentityDetectorReversedPeopleDict].([]string)
		}
	} else if exists {
		analyser.PeopleNumber = 0
	}
	analyser.Debug = false
}

// Flag for the command line switch which enables this analysis.
func (analyser *BurndownAnalysis) Flag() string {
	return "burndown"
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *BurndownAnalysis) Initialize(repository *git.Repository) {
	if analyser.Granularity <= 0 {
		fmt.Fprintf(os.Stderr, "Warning: adjusted the granularity to %d days\n",
			DefaultBurndownGranularity)
		analyser.Granularity = DefaultBurndownGranularity
	}
	if analyser.Sampling <= 0 {
		fmt.Fprintf(os.Stderr, "Warning: adjusted the sampling to %d days\n",
			DefaultBurndownGranularity)
		analyser.Sampling = DefaultBurndownGranularity
	}
	if analyser.Sampling > analyser.Granularity {
		fmt.Fprintf(os.Stderr, "Warning: granularity may not be less than sampling, adjusted to %d\n",
			analyser.Granularity)
		analyser.Sampling = analyser.Granularity
	}
	analyser.repository = repository
	analyser.globalStatus = newGlobalCounter()
	analyser.fileHistories = map[string][][]int64{}
	analyser.files = make(map[string]map[string]*File)
	analyser.matrix = make([]map[int]int64, analyser.PeopleNumber)
	analyser.people = newPeopleCounter(analyser.PeopleNumber)
}

type changeProcessor interface {
	Process([]*object.Change) (map[string]*File, error)
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, "commit" is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *BurndownAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	commitHash := commit.Hash.String()

	// defer func() {
	// 	fmt.Println(strings.TrimSpace(commit.Message), "global", analyser.globalStatus.diffs, "people", analyser.people.diffs)
	// }()
	// fmt.Println("process", commit.Hash, removeSignedOffBy(commit.Message))

	author := deps[DependencyAuthor].(int)
	day := deps[DependencyDay].(int)
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps[DependencyTreeChanges].(object.Changes)
	fileDiffs := deps[DependencyFileDiff].(map[string]FileDiffData)

	makeProcessor := func(files map[string]*File) *changeApplier {
		return &changeApplier{
			day:    day,
			author: author,
			makeStatuses: func() []Status {
				statuses := make([]Status, 1)
				statuses[0] = NewStatus(nil, analyser.updateStatus)
				// if analyser.TrackFiles {
				// 	statuses = append(statuses, NewStatus(map[int]int64{}, analyser.updateStatus))
				// }
				if analyser.PeopleNumber > 0 {
					statuses = append(statuses, NewStatus(nil, analyser.updatePeople))
					//statuses = append(statuses, NewStatus(matrix, analyser.updateMatrix))
				}
				return statuses
			},

			files:     files,
			fileDiffs: fileDiffs,
			cache:     cache,

			Debug: analyser.Debug,
		}
	}

	analyser.commitDay = day

	var parentCommitHash string
	var processor changeProcessor
	switch len(commit.ParentHashes) {
	case 0: // initial commit
		processor = makeProcessor(make(map[string]*File))

	case 1: // regular commit
		parentCommitHash = commit.ParentHashes[0].String()
		files, ok := analyser.files[parentCommitHash]
		if !ok {
			return nil, fmt.Errorf("commit with hash %s wasn't processed (required by %s)", parentCommitHash, commitHash)
		}

		processor = makeProcessor(copyFiles(files))

	case 2: // merge commit
		parentCommitHash = commit.ParentHashes[0].String()
		files1, ok := analyser.files[parentCommitHash]
		if !ok {
			return nil, fmt.Errorf("commit with hash %s wasn't processed (required by %s)", parentCommitHash, commitHash)
		}
		parentCommitHash = commit.ParentHashes[1].String()
		files2, ok := analyser.files[parentCommitHash]
		if !ok {
			return nil, fmt.Errorf("commit with hash %s wasn't processed (required by %s)", parentCommitHash, commitHash)
		}

		processor = &changeMerger{
			files:     copyFiles(files1),
			sideFiles: copyFiles(files2),

			fileDiffs: fileDiffs,
			cache:     cache,

			Debug: analyser.Debug,
		}

	default:
		return nil, fmt.Errorf("commit has more than 2 parents")
	}

	files, err := processor.Process(treeDiffs)
	if err != nil {
		return nil, err
	}
	analyser.files[commitHash] = files

	analyser.cleanup(commit, deps["commitDeps"].(map[plumbing.Hash]int))

	return nil, nil
}

// remove files for commits we don't need anymore
func (analyser *BurndownAnalysis) cleanup(commit *object.Commit, commitDeps map[plumbing.Hash]int) {
	for _, h := range commit.ParentHashes {
		commitDeps[h]--
		if commitDeps[h] == 0 {
			delete(commitDeps, h)
			delete(analyser.files, h.String())
		}
	}
}

func copyFiles(files map[string]*File) map[string]*File {
	copiedFiles := make(map[string]*File, len(files))
	for name, file := range files {
		copiedFiles[name] = file.Copy()
	}
	return copiedFiles
}

func (analyser *BurndownAnalysis) updateStatus(
	_ interface{}, _ int, previousValue int, delta int) {
	_, previousTime := unpackPersonWithDay(previousValue)
	analyser.globalStatus.update(analyser.commitDay, previousTime, int64(delta))
}

func (analyser *BurndownAnalysis) updatePeople(
	_ interface{}, _ int, previousValue int, delta int) {
	previousAuthor, previousTime := unpackPersonWithDay(previousValue)
	if previousAuthor == AuthorMissing {
		return
	}
	analyser.people.update(analyser.commitDay, previousAuthor, previousTime, int64(delta))
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (analyser *BurndownAnalysis) Finalize() interface{} {
	return BurndownResult{
		GlobalHistory:      analyser.globalStatus.matrix(analyser.Sampling, analyser.Granularity),
		FileHistories:      analyser.fileHistories,
		PeopleHistories:    analyser.people.matrix(analyser.Sampling, analyser.Granularity),
		reversedPeopleDict: analyser.reversedPeopleDict,
		sampling:           analyser.Sampling,
		granularity:        analyser.Granularity,
	}
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *BurndownAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	burndownResult := result.(BurndownResult)
	if binary {
		return analyser.serializeBinary(&burndownResult, writer)
	}
	analyser.serializeText(&burndownResult, writer)
	return nil
}

func (analyser *BurndownAnalysis) serializeText(result *BurndownResult, writer io.Writer) {
	fmt.Fprintln(writer, "  granularity:", result.granularity)
	fmt.Fprintln(writer, "  sampling:", result.sampling)
	yaml.PrintMatrix(writer, result.GlobalHistory, 2, "project", true)
	if len(result.FileHistories) > 0 {
		fmt.Fprintln(writer, "  files:")
		keys := sortedKeys(result.FileHistories)
		for _, key := range keys {
			yaml.PrintMatrix(writer, result.FileHistories[key], 4, key, true)
		}
	}

	if len(result.PeopleHistories) > 0 {
		fmt.Fprintln(writer, "  people_sequence:")
		for key := range result.PeopleHistories {
			fmt.Fprintln(writer, "    - "+yaml.SafeString(result.reversedPeopleDict[key]))
		}
		fmt.Fprintln(writer, "  people:")
		for key, val := range result.PeopleHistories {
			yaml.PrintMatrix(writer, val, 4, result.reversedPeopleDict[key], true)
		}
		if len(result.PeopleMatrix) > 0 {
			fmt.Fprintln(writer, "  people_interaction: |-")
			yaml.PrintMatrix(writer, result.PeopleMatrix, 4, "", false)
		}
	}
}

func (analyser *BurndownAnalysis) serializeBinary(result *BurndownResult, writer io.Writer) error {
	message := pb.BurndownAnalysisResults{
		Granularity: int32(result.granularity),
		Sampling:    int32(result.sampling),
	}
	if len(result.GlobalHistory) > 0 {
		message.Project = pb.ToBurndownSparseMatrix(result.GlobalHistory, "project")
	}
	if len(result.FileHistories) > 0 {
		message.Files = make([]*pb.BurndownSparseMatrix, len(result.FileHistories))
		keys := sortedKeys(result.FileHistories)
		i := 0
		for _, key := range keys {
			message.Files[i] = pb.ToBurndownSparseMatrix(
				result.FileHistories[key], key)
			i++
		}
	}

	if len(result.PeopleHistories) > 0 {
		message.People = make(
			[]*pb.BurndownSparseMatrix, len(result.PeopleHistories))
		for key, val := range result.PeopleHistories {
			if len(val) > 0 {
				message.People[key] = pb.ToBurndownSparseMatrix(val, result.reversedPeopleDict[key])
			}
		}
		message.PeopleInteraction = pb.DenseToCompressedSparseRowMatrix(result.PeopleMatrix)
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

// Deserialize converts the specified protobuf bytes to BurndownResult.
func (analyser *BurndownAnalysis) Deserialize(pbmessage []byte) (interface{}, error) {
	msg := pb.BurndownAnalysisResults{}
	err := proto.Unmarshal(pbmessage, &msg)
	if err != nil {
		return nil, err
	}
	result := BurndownResult{}
	convertCSR := func(mat *pb.BurndownSparseMatrix) [][]int64 {
		res := make([][]int64, mat.NumberOfRows)
		for i := 0; i < int(mat.NumberOfRows); i++ {
			res[i] = make([]int64, mat.NumberOfColumns)
			for j := 0; j < len(mat.Rows[i].Columns); j++ {
				res[i][j] = int64(mat.Rows[i].Columns[j])
			}
		}
		return res
	}
	result.GlobalHistory = convertCSR(msg.Project)
	result.FileHistories = map[string][][]int64{}
	for _, mat := range msg.Files {
		result.FileHistories[mat.Name] = convertCSR(mat)
	}
	result.reversedPeopleDict = make([]string, len(msg.People))
	result.PeopleHistories = make([][][]int64, len(msg.People))
	for i, mat := range msg.People {
		result.PeopleHistories[i] = convertCSR(mat)
		result.reversedPeopleDict[i] = mat.Name
	}
	if msg.PeopleInteraction != nil {
		result.PeopleMatrix = make([][]int64, msg.PeopleInteraction.NumberOfRows)
	}
	for i := 0; i < len(result.PeopleMatrix); i++ {
		result.PeopleMatrix[i] = make([]int64, msg.PeopleInteraction.NumberOfColumns)
		for j := int(msg.PeopleInteraction.Indptr[i]); j < int(msg.PeopleInteraction.Indptr[i+1]); j++ {
			result.PeopleMatrix[i][msg.PeopleInteraction.Indices[j]] = msg.PeopleInteraction.Data[j]
		}
	}
	result.sampling = int(msg.Sampling)
	result.granularity = int(msg.Granularity)
	return result, nil
}

func packPersonWithDay(person int, day int) int {
	result := day
	result |= person << 14
	// This effectively means max 16384 days (>44 years) and (131072 - 2) devs
	return result
}

func unpackPersonWithDay(value int) (int, int) {
	return value >> 14, value & 0x3FFF
}

func init() {
	Registry.Register(&BurndownAnalysis{})
}
