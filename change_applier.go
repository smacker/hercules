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

type changeApplier struct {
	day          int
	author       int
	makeStatuses func() []Status

	files     map[string]*File
	fileDiffs map[string]FileDiffData
	cache     map[plumbing.Hash]*object.Blob

	Debug bool
}

func (p *changeApplier) Process(changes []*object.Change) (map[string]*File, error) {
	for _, change := range changes {
		if err := p.processChange(change); err != nil {
			return nil, err
		}
	}

	return p.files, nil
}

func (p *changeApplier) processChange(change *object.Change) error {
	action, _ := change.Action()
	nameTo := change.To.Name
	nameFrom := change.From.Name

	switch action {
	case merkletrie.Insert:
		lines, err := CountLines(p.cache[change.To.TreeEntry.Hash])
		if err != nil {
			if err.Error() == "binary" {
				return nil
			}
			return err
		}
		return p.handleInsertion(nameTo, lines)
	case merkletrie.Delete:
		lines, err := CountLines(p.cache[change.From.TreeEntry.Hash])
		if err != nil {
			if err.Error() == "binary" {
				return nil
			}
			return err
		}
		return p.handleDelete(nameFrom, lines)
	case merkletrie.Modify:
		lines, err := CountLines(p.cache[change.To.TreeEntry.Hash])
		if err != nil {
			if err.Error() == "binary" {
				return nil
			}
			return err
		}
		return p.handleModification(nameFrom, nameTo, lines)
	default:
		return errors.New("unknown action")
	}
}

func (p *changeApplier) handleInsertion(name string, lines int) error {
	_, exists := p.files[name]
	if exists {
		return fmt.Errorf("file %s already exists", name)
	}
	p.files[name] = NewFile(packPersonWithDay(p.author, p.day), lines, p.makeStatuses()...)
	return nil
}

func (p *changeApplier) handleDelete(name string, lines int) error {
	file, exists := p.files[name]
	if !exists {
		return fmt.Errorf("file %s doesn't exist", name)
	}
	file.Update(packPersonWithDay(p.author, p.day), 0, 0, lines, false)
	delete(p.files, name)
	return nil
}

func (p *changeApplier) handleModification(nameFrom, nameTo string, lines int) error {
	if nameFrom == "" {
		return p.handleInsertion(nameTo, lines)
	}

	// possible rename
	if nameFrom != nameTo {
		err := p.handleRename(nameFrom, nameTo)
		if err != nil {
			return err
		}
	}

	file := p.files[nameTo]
	thisDiffs := p.fileDiffs[nameTo]
	if file.Len() != thisDiffs.OldLinesOfCode {
		fmt.Fprintf(os.Stderr, "====TREE====\n%s", file.Dump())
		return fmt.Errorf("regular before: %s: internal integrity error src %d != %d",
			nameTo, thisDiffs.OldLinesOfCode, file.Len())
	}

	// we do not call RunesToDiffLines so the number of lines equals
	// to the rune count
	position := 0
	pending := diffmatchpatch.Diff{Text: ""}

	apply := func(edit diffmatchpatch.Diff) {
		length := utf8.RuneCountInString(edit.Text)
		if edit.Type == diffmatchpatch.DiffInsert {
			file.Update(packPersonWithDay(p.author, p.day), position, length, 0, false)
			position += length
		} else {
			file.Update(packPersonWithDay(p.author, p.day), position, 0, length, false)
		}
		if p.Debug {
			file.Validate()
		}
	}

	for _, edit := range thisDiffs.Diffs {
		dumpBefore := ""
		if p.Debug {
			dumpBefore = file.Dump()
		}
		length := utf8.RuneCountInString(edit.Text)
		debugError := func() {
			fmt.Fprintf(os.Stderr, "%s: internal diff error\n", nameTo)
			fmt.Fprintf(os.Stderr, "Update(%d, %d, %d (0), %d (0))\n", p.day, position,
				length, utf8.RuneCountInString(pending.Text))
			if dumpBefore != "" {
				fmt.Fprintf(os.Stderr, "====TREE BEFORE====\n%s====END====\n", dumpBefore)
			}
			fmt.Fprintf(os.Stderr, "====TREE AFTER====\n%s====END====\n", file.Dump())
		}
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
					debugError()
					return errors.New("DiffInsert may not appear after DiffInsert")
				}
				file.Update(packPersonWithDay(p.author, p.day), position, length,
					utf8.RuneCountInString(pending.Text), false)
				if p.Debug {
					file.Validate()
				}
				position += length
				pending.Text = ""
			} else {
				pending = edit
			}
		case diffmatchpatch.DiffDelete:
			if pending.Text != "" {
				debugError()
				return errors.New("DiffDelete may not appear after DiffInsert/DiffDelete")
			}
			pending = edit
		default:
			debugError()
			return fmt.Errorf("diff operation is not supported: %d", edit.Type)
		}
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}

	if file.Len() != thisDiffs.NewLinesOfCode {
		return fmt.Errorf("regular after: %s: internal integrity error dst %d != %d",
			nameTo, thisDiffs.NewLinesOfCode, file.Len())
	}

	return nil
}

func (p *changeApplier) handleRename(from, to string) error {
	file, exists := p.files[from]
	if !exists {
		return fmt.Errorf("file %s does not exist", from)
	}
	p.files[to] = file
	delete(p.files, from)
	return nil
}
