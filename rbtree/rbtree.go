package rbtree

//
// Public definitions
//

// Item is the object stored in each tree node.
type Item struct {
	Key   int
	Value int
}

// RBTree created by Yaz Saito on 06/10/12.
//
// A red-black tree with an API similar to C++ STL's.
//
// The implementation is inspired (read: stolen) from:
// http://en.literateprograms.org/Red-black_tree_(C)#chunk use:private function prototypes.
//
// The code was optimized for the simple integer types of Key and Value.
type RBTree struct {
	// Root of the tree
	root *node

	// The minimum and maximum nodes under the root.
	minNode, maxNode *node

	// Number of nodes under root, including the root
	count int
}

func (root *RBTree) Copy() *RBTree {
	minItem := root.minNode.item
	maxItem := root.maxNode.item

	treeCopy := &RBTree{
		root:  root.root.copy(),
		count: root.count,
	}

	nodes := []*node{treeCopy.root}
	for len(nodes) > 0 {
		n := nodes[0]
		nodes = nodes[1:]
		if n.item == minItem {
			treeCopy.minNode = n
		}
		if n.item == maxItem {
			treeCopy.maxNode = n
		}
		if treeCopy.minNode != nil && treeCopy.maxNode != nil {
			break
		}
		if n.left != nil {
			nodes = append(nodes, n.left)
		}
		if n.right != nil {
			nodes = append(nodes, n.right)
		}
	}

	return treeCopy
}

// Len returns the number of elements in the tree.
func (root *RBTree) Len() int {
	return root.count
}

// Get is a convenience function for finding an element equal to Key. Returns
// nil if not found.
func (root *RBTree) Get(key int) *int {
	n, exact := root.findGE(key)
	if exact {
		return &n.item.Value
	}
	return nil
}

// Min creates an iterator that points to the minimum item in the tree.
// If the tree is empty, returns Limit()
func (root *RBTree) Min() Iterator {
	return Iterator{root, root.minNode}
}

// Max creates an iterator that points at the maximum item in the tree.
//
// If the tree is empty, returns NegativeLimit().
func (root *RBTree) Max() Iterator {
	if root.maxNode == nil {
		return Iterator{root, negativeLimitNode}
	}
	return Iterator{root, root.maxNode}
}

// Limit creates an iterator that points beyond the maximum item in the tree.
func (root *RBTree) Limit() Iterator {
	return Iterator{root, nil}
}

// NegativeLimit creates an iterator that points before the minimum item in the tree.
func (root *RBTree) NegativeLimit() Iterator {
	return Iterator{root, negativeLimitNode}
}

// FindGE finds the smallest element N such that N >= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns root.Limit().
func (root *RBTree) FindGE(key int) Iterator {
	n, _ := root.findGE(key)
	return Iterator{root, n}
}

// FindLE finds the largest element N such that N <= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns iter.NegativeLimit().
func (root *RBTree) FindLE(key int) Iterator {
	n, exact := root.findGE(key)
	if exact {
		return Iterator{root, n}
	}
	if n != nil {
		return Iterator{root, n.doPrev()}
	}
	if root.maxNode == nil {
		return Iterator{root, negativeLimitNode}
	}
	return Iterator{root, root.maxNode}
}

// Insert an item. If the item is already in the tree, do nothing and
// return false. Else return true.
func (root *RBTree) Insert(item Item) (bool, Iterator) {
	// TODO: delay creating n until it is found to be inserted
	n := root.doInsert(item)
	if n == nil {
		return false, Iterator{}
	}
	insN := n

	n.color = red

	for true {
		// Case 1: N is at the root
		if n.parent == nil {
			n.color = black
			break
		}

		// Case 2: The parent is black, so the tree already
		// satisfies the RB properties
		if n.parent.color == black {
			break
		}

		// Case 3: parent and uncle are both red.
		// Then paint both black and make grandparent red.
		grandparent := n.parent.parent
		var uncle *node
		if n.parent.isLeftChild() {
			uncle = grandparent.right
		} else {
			uncle = grandparent.left
		}
		if uncle != nil && uncle.color == red {
			n.parent.color = black
			uncle.color = black
			grandparent.color = red
			n = grandparent
			continue
		}

		// Case 4: parent is red, uncle is black (1)
		if n.isRightChild() && n.parent.isLeftChild() {
			root.rotateLeft(n.parent)
			n = n.left
			continue
		}
		if n.isLeftChild() && n.parent.isRightChild() {
			root.rotateRight(n.parent)
			n = n.right
			continue
		}

		// Case 5: parent is read, uncle is black (2)
		n.parent.color = black
		grandparent.color = red
		if n.isLeftChild() {
			root.rotateRight(grandparent)
		} else {
			root.rotateLeft(grandparent)
		}
		break
	}
	return true, Iterator{root, insN}
}

// DeleteWithKey deletes an item with the given Key. Returns true iff the item was
// found.
func (root *RBTree) DeleteWithKey(key int) bool {
	iter := root.FindGE(key)
	if iter.node != nil {
		root.DeleteWithIterator(iter)
		return true
	}
	return false
}

// DeleteWithIterator deletes the current item.
//
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (root *RBTree) DeleteWithIterator(iter Iterator) {
	doAssert(!iter.Limit() && !iter.NegativeLimit())
	root.doDelete(iter.node)
}

// Iterator allows scanning tree elements in sort order.
//
// Iterator invalidation rule is the same as C++ std::map<>'s. That
// is, if you delete the element that an iterator points to, the
// iterator becomes invalid. For other operation types, the iterator
// remains valid.
type Iterator struct {
	root *RBTree
	node *node
}

// Equal checks for the underlying nodes equality.
func (iter Iterator) Equal(other Iterator) bool {
	return iter.node == other.node
}

// Limit checks if the iterator points beyond the max element in the tree.
func (iter Iterator) Limit() bool {
	return iter.node == nil
}

// Min checks if the iterator points to the minimum element in the tree.
func (iter Iterator) Min() bool {
	return iter.node == iter.root.minNode
}

// Max checks if the iterator points to the maximum element in the tree.
func (iter Iterator) Max() bool {
	return iter.node == iter.root.maxNode
}

// NegativeLimit checks if the iterator points before the minimum element in the tree.
func (iter Iterator) NegativeLimit() bool {
	return iter.node == negativeLimitNode
}

// Item returns the current element. Allows mutating the node
// (key to be changed with care!).
//
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (iter Iterator) Item() *Item {
	return &iter.node.item
}

// Next creates a new iterator that points to the successor of the current element.
//
// REQUIRES: !iter.Limit()
func (iter Iterator) Next() Iterator {
	doAssert(!iter.Limit())
	if iter.NegativeLimit() {
		return Iterator{iter.root, iter.root.minNode}
	}
	return Iterator{iter.root, iter.node.doNext()}
}

// Prev creates a new iterator that points to the predecessor of the current
// node.
//
// REQUIRES: !iter.NegativeLimit()
func (iter Iterator) Prev() Iterator {
	doAssert(!iter.NegativeLimit())
	if !iter.Limit() {
		return Iterator{iter.root, iter.node.doPrev()}
	}
	if iter.root.maxNode == nil {
		return Iterator{iter.root, negativeLimitNode}
	}
	return Iterator{iter.root, iter.root.maxNode}
}

func doAssert(b bool) {
	if !b {
		panic("rbtree internal assertion failed")
	}
}

const red = iota
const black = 1 + iota

type node struct {
	item                Item
	parent, left, right *node
	color               int // black or red
}

func (n *node) copy() *node {
	copyN := *n
	if n.left != nil {
		copyN.left = n.left.copy()
		copyN.left.parent = &copyN
	}
	if n.right != nil {
		copyN.right = n.right.copy()
		copyN.right.parent = &copyN
	}

	return &copyN
}

var negativeLimitNode *node

//
// Internal node attribute accessors
//
func getColor(n *node) int {
	if n == nil {
		return black
	}
	return n.color
}

func (n *node) isLeftChild() bool {
	return n == n.parent.left
}

func (n *node) isRightChild() bool {
	return n == n.parent.right
}

func (n *node) sibling() *node {
	doAssert(n.parent != nil)
	if n.isLeftChild() {
		return n.parent.right
	}
	return n.parent.left
}

// Return the minimum node that's larger than N. Return nil if no such
// node is found.
func (n *node) doNext() *node {
	if n.right != nil {
		m := n.right
		for m.left != nil {
			m = m.left
		}
		return m
	}

	for n != nil {
		p := n.parent
		if p == nil {
			return nil
		}
		if n.isLeftChild() {
			return p
		}
		n = p
	}
	return nil
}

// Return the maximum node that's smaller than N. Return nil if no
// such node is found.
func (n *node) doPrev() *node {
	if n.left != nil {
		return maxPredecessor(n)
	}

	for n != nil {
		p := n.parent
		if p == nil {
			break
		}
		if n.isRightChild() {
			return p
		}
		n = p
	}
	return negativeLimitNode
}

// Return the predecessor of "n".
func maxPredecessor(n *node) *node {
	doAssert(n.left != nil)
	m := n.left
	for m.right != nil {
		m = m.right
	}
	return m
}

//
// Tree methods
//

//
// Private methods
//

func (root *RBTree) recomputeMinNode() {
	root.minNode = root.root
	if root.minNode != nil {
		for root.minNode.left != nil {
			root.minNode = root.minNode.left
		}
	}
}

func (root *RBTree) recomputeMaxNode() {
	root.maxNode = root.root
	if root.maxNode != nil {
		for root.maxNode.right != nil {
			root.maxNode = root.maxNode.right
		}
	}
}

func (root *RBTree) maybeSetMinNode(n *node) {
	if root.minNode == nil {
		root.minNode = n
		root.maxNode = n
	} else if n.item.Key < root.minNode.item.Key {
		root.minNode = n
	}
}

func (root *RBTree) maybeSetMaxNode(n *node) {
	if root.maxNode == nil {
		root.minNode = n
		root.maxNode = n
	} else if n.item.Key > root.maxNode.item.Key {
		root.maxNode = n
	}
}

// Try inserting "item" into the tree. Return nil if the item is
// already in the tree. Otherwise return a new (leaf) node.
func (root *RBTree) doInsert(item Item) *node {
	if root.root == nil {
		n := &node{item: item}
		root.root = n
		root.minNode = n
		root.maxNode = n
		root.count++
		return n
	}
	parent := root.root
	for true {
		comp := item.Key - parent.item.Key
		if comp == 0 {
			return nil
		} else if comp < 0 {
			if parent.left == nil {
				n := &node{item: item, parent: parent}
				parent.left = n
				root.count++
				root.maybeSetMinNode(n)
				return n
			}
			parent = parent.left
		} else {
			if parent.right == nil {
				n := &node{item: item, parent: parent}
				parent.right = n
				root.count++
				root.maybeSetMaxNode(n)
				return n
			}
			parent = parent.right
		}
	}
	panic("should not reach here")
}

// Find a node whose item >= Key. The 2nd return Value is true iff the
// node.item==Key. Returns (nil, false) if all nodes in the tree are <
// Key.
func (root *RBTree) findGE(key int) (*node, bool) {
	n := root.root
	for true {
		if n == nil {
			return nil, false
		}
		comp := key - n.item.Key
		if comp == 0 {
			return n, true
		} else if comp < 0 {
			if n.left != nil {
				n = n.left
			} else {
				return n, false
			}
		} else {
			if n.right != nil {
				n = n.right
			} else {
				succ := n.doNext()
				if succ == nil {
					return nil, false
				}
				return succ, key == succ.item.Key
			}
		}
	}
	panic("should not reach here")
}

// Delete N from the tree.
func (root *RBTree) doDelete(n *node) {
	if n.left != nil && n.right != nil {
		pred := maxPredecessor(n)
		root.swapNodes(n, pred)
	}

	doAssert(n.left == nil || n.right == nil)
	child := n.right
	if child == nil {
		child = n.left
	}
	if n.color == black {
		n.color = getColor(child)
		root.deleteCase1(n)
	}
	root.replaceNode(n, child)
	if n.parent == nil && child != nil {
		child.color = black
	}
	root.count--
	if root.count == 0 {
		root.minNode = nil
		root.maxNode = nil
	} else {
		if root.minNode == n {
			root.recomputeMinNode()
		}
		if root.maxNode == n {
			root.recomputeMaxNode()
		}
	}
}

// Move n to the pred's place, and vice versa
//
func (root *RBTree) swapNodes(n, pred *node) {
	doAssert(pred != n)
	isLeft := pred.isLeftChild()
	tmp := *pred
	root.replaceNode(n, pred)
	pred.color = n.color

	if tmp.parent == n {
		// swap the positions of n and pred
		if isLeft {
			pred.left = n
			pred.right = n.right
			if pred.right != nil {
				pred.right.parent = pred
			}
		} else {
			pred.left = n.left
			if pred.left != nil {
				pred.left.parent = pred
			}
			pred.right = n
		}
		n.item = tmp.item
		n.parent = pred

		n.left = tmp.left
		if n.left != nil {
			n.left.parent = n
		}
		n.right = tmp.right
		if n.right != nil {
			n.right.parent = n
		}
	} else {
		pred.left = n.left
		if pred.left != nil {
			pred.left.parent = pred
		}
		pred.right = n.right
		if pred.right != nil {
			pred.right.parent = pred
		}
		if isLeft {
			tmp.parent.left = n
		} else {
			tmp.parent.right = n
		}
		n.item = tmp.item
		n.parent = tmp.parent
		n.left = tmp.left
		if n.left != nil {
			n.left.parent = n
		}
		n.right = tmp.right
		if n.right != nil {
			n.right.parent = n
		}
	}
	n.color = tmp.color
}

func (root *RBTree) deleteCase1(n *node) {
	for true {
		if n.parent != nil {
			if getColor(n.sibling()) == red {
				n.parent.color = red
				n.sibling().color = black
				if n == n.parent.left {
					root.rotateLeft(n.parent)
				} else {
					root.rotateRight(n.parent)
				}
			}
			if getColor(n.parent) == black &&
				getColor(n.sibling()) == black &&
				getColor(n.sibling().left) == black &&
				getColor(n.sibling().right) == black {
				n.sibling().color = red
				n = n.parent
				continue
			} else {
				// case 4
				if getColor(n.parent) == red &&
					getColor(n.sibling()) == black &&
					getColor(n.sibling().left) == black &&
					getColor(n.sibling().right) == black {
					n.sibling().color = red
					n.parent.color = black
				} else {
					root.deleteCase5(n)
				}
			}
		}
		break
	}
}

func (root *RBTree) deleteCase5(n *node) {
	if n == n.parent.left &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().left) == red &&
		getColor(n.sibling().right) == black {
		n.sibling().color = red
		n.sibling().left.color = black
		root.rotateRight(n.sibling())
	} else if n == n.parent.right &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().right) == red &&
		getColor(n.sibling().left) == black {
		n.sibling().color = red
		n.sibling().right.color = black
		root.rotateLeft(n.sibling())
	}

	// case 6
	n.sibling().color = getColor(n.parent)
	n.parent.color = black
	if n == n.parent.left {
		doAssert(getColor(n.sibling().right) == red)
		n.sibling().right.color = black
		root.rotateLeft(n.parent)
	} else {
		doAssert(getColor(n.sibling().left) == red)
		n.sibling().left.color = black
		root.rotateRight(n.parent)
	}
}

func (root *RBTree) replaceNode(oldn, newn *node) {
	if oldn.parent == nil {
		root.root = newn
	} else {
		if oldn == oldn.parent.left {
			oldn.parent.left = newn
		} else {
			oldn.parent.right = newn
		}
	}
	if newn != nil {
		newn.parent = oldn.parent
	}
}

/*
    X		     Y
  A   Y	    =>     X   C
     B C 	  A B
*/
func (root *RBTree) rotateLeft(x *node) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		root.root = y
	} else {
		if x.isLeftChild() {
			x.parent.left = y
		} else {
			x.parent.right = y
		}
	}
	y.left = x
	x.parent = y
}

/*
     Y           X
   X   C  =>   A   Y
  A B             B C
*/
func (root *RBTree) rotateRight(y *node) {
	x := y.left

	// Move "B"
	y.left = x.right
	if x.right != nil {
		x.right.parent = y
	}

	x.parent = y.parent
	if y.parent == nil {
		root.root = x
	} else {
		if y.isLeftChild() {
			y.parent.left = x
		} else {
			y.parent.right = x
		}
	}
	x.right = y
	y.parent = x
}

func init() {
	negativeLimitNode = &node{}
}
