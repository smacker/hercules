package hercules

import (
	"errors"
	"fmt"
	"os"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type changeMerger struct {
	files     map[string]*File
	sideFiles map[string]*File

	fileDiffs map[string]FileDiffData
	cache     map[plumbing.Hash]*object.Blob

	Debug bool
}

func (p *changeMerger) Process(changes []*object.Change) (map[string]*File, error) {
	for _, change := range changes {
		if err := p.processChange(change); err != nil {
			return nil, err
		}
	}

	return p.files, nil
}

func (p *changeMerger) processChange(change *object.Change) error {
	action, _ := change.Action()
	nameTo := change.To.Name
	nameFrom := change.From.Name

	// No need to update counters, they we updated during regular-commits update
	// In case of merge conflicts changes introduced by merge commit aren't counted
	// it isn't correct and need to be fixed later
	//
	// For example:
	// if the same line was removed 2 times, author lose 2 line (not 1)
	// if the same line was added 2 time, authors get +1 lines each, the same authot gets +2

	switch action {
	case merkletrie.Insert:
		b, err := isBinary(p.cache[change.To.TreeEntry.Hash])
		if err != nil {
			return err
		}
		if b {
			return nil
		}
		if p.sideFiles[nameTo] == nil {
			return fmt.Errorf("file %s not found in side files", nameTo)
		}
		p.files[nameTo] = p.sideFiles[nameTo]
	case merkletrie.Delete:
		b, err := isBinary(p.cache[change.From.TreeEntry.Hash])
		if err != nil {
			return err
		}
		if b {
			return nil
		}
		if _, exists := p.files[nameFrom]; !exists {
			return fmt.Errorf("file %s doesn't exist", nameFrom)
		}
		delete(p.files, nameFrom)
		return nil
	case merkletrie.Modify:
		b, err := isBinary(p.cache[change.To.TreeEntry.Hash])
		if err != nil {
			return err
		}
		if b {
			return nil
		}
		return p.handleModification(nameFrom, nameTo)
	default:
		return errors.New("unknown action")
	}

	return nil
}

func (p *changeMerger) handleModification(nameFrom, nameTo string) error {
	if nameFrom == "" {
		newFile := p.sideFiles[nameFrom]
		if newFile == nil {
			return fmt.Errorf("file %s doesn't exist", nameFrom)
		}
		p.files[nameFrom] = newFile
		return nil
	}

	// possible rename
	if nameFrom != nameTo {
		if p.sideFiles[nameTo] == nil {
			return fmt.Errorf("file %s doesn't exist in side files", nameTo)
		}
		if p.files[nameFrom] == nil {
			return fmt.Errorf("file %s doesn't exist", nameFrom)
		}
		p.files[nameTo] = p.files[nameFrom]
		delete(p.files, nameFrom)
	}

	file := p.files[nameTo]
	file2 := p.sideFiles[nameTo]
	thisDiffs := p.fileDiffs[nameTo]
	if file.Len() != thisDiffs.OldLinesOfCode {
		fmt.Fprintf(os.Stderr, "====TREE1====\n%s", file.Dump())
		return fmt.Errorf("before merge: %s: internal integrity error src %d != %d",
			nameTo, thisDiffs.OldLinesOfCode, file.Len())
	}

	position := 0
	pending := diffmatchpatch.Diff{Text: ""}

	apply := func(edit diffmatchpatch.Diff) {
		length := utf8.RuneCountInString(edit.Text)
		if edit.Type == diffmatchpatch.DiffInsert {
			timeVal, err := getTimeVal(file2, position)
			if err != nil {
				panic(err)
			}

			file.Update(timeVal, position, length, 0, true)
			position += length
		} else {
			// In merge we don't update counters
			// So for delete we don't care about time value
			file.Update(0, position, 0, length, true)
		}
		if p.Debug {
			file.Validate()
		}
	}

	for _, edit := range thisDiffs.Diffs {
		length := utf8.RuneCountInString(edit.Text)
		switch edit.Type {
		case diffmatchpatch.DiffEqual:
			if pending.Text != "" {
				apply(pending)
				pending.Text = ""
			}
			position += length
		case diffmatchpatch.DiffInsert:
			if pending.Text != "" {
				if pending.Type == diffmatchpatch.DiffInsert {
					return errors.New("DiffInsert may not appear after DiffInsert")
				}
				timeVal, err := getTimeVal(file2, position)
				if err != nil {
					return err
				}

				file.Update(timeVal, position, length, utf8.RuneCountInString(pending.Text), true)
				position += length
				pending.Text = ""
			} else {
				pending = edit
			}
		case diffmatchpatch.DiffDelete:
			if pending.Text != "" {
				return errors.New("DiffDelete may not appear after DiffInsert/DiffDelete")
			}
			pending = edit
		default:
			return fmt.Errorf("diff operation is not supported: %d", edit.Type)
		}
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}
	if file.Len() != thisDiffs.NewLinesOfCode {
		return fmt.Errorf("merge %s: internal integrity error dst %d != %d",
			nameTo, thisDiffs.NewLinesOfCode, file.Len())
	}

	return nil
}

func getTimeVal(file *File, position int) (int, error) {
	// TODO try to avoid using internal properties
	iter := file.tree.FindLE(position)
	if iter.Limit() {
		return 0, errors.New("Limit")
	}
	if iter.NegativeLimit() {
		return 0, errors.New("NegativeLimit")
	}
	timeVal := iter.Item().Value
	if timeVal == -1 {
		timeVal = iter.Prev().Item().Value
	}
	return timeVal, nil
}
