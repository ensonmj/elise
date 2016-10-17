package htmlutil

import "golang.org/x/net/html"

// TrimNode delete node from down to top according to needTrim
// a--b--c: if after trim 'c', needTrim(b) return true, 'c' will be trimed
func TrimNode(n *html.Node, needTrim func(n *html.Node) bool) {
	var next *html.Node
	for c := n.FirstChild; c != nil; c = next {
		next = c.NextSibling
		TrimNode(c, needTrim)
	}
	if needTrim(n) {
		n.Parent.RemoveChild(n)
	}
}

// ExtractIsomorphisms extract isomorphic nodes from html node tree
//      c0
//     /
//    /
// a--b--c2--d0     =>   b--c2--d0
//    \   \               \  \
//     \   d1--e    =>     \  d1--e
//      c2--d0              c2--d0
//        \                  \
//         d1--e              d1--e
func ExtractIsomorphisms(root *html.Node, leafEqual func(c, n *html.Node) bool) []*html.Node {
	if !isomorphic(root, leafEqual) {
		return []*html.Node{root}
	}

	// find node to be splitted
	for root.FirstChild.NextSibling == nil {
		root = root.FirstChild
	}

	var grpImgs []*html.Node
	newRoot := &html.Node{Type: html.ElementNode, Data: "div"}
	begin := root.FirstChild
	var end, next *html.Node
	for curr := root.FirstChild; curr != nil; curr = next {
		curr.Parent = newRoot
		next = curr.NextSibling

		if next != nil {
			if NodeEqual(curr, next, leafEqual) {
				continue
			}
			curr.NextSibling = nil
			next.PrevSibling = nil
		}

		end = curr
		newRoot.FirstChild = begin
		if begin != end {
			newRoot.LastChild = end
		}
		grpImgs = append(grpImgs, newRoot)

		begin = next
		newRoot = &html.Node{Type: html.ElementNode, Data: "div"}
	}

	var allGrpImgs []*html.Node
	for _, n := range grpImgs {
		allGrpImgs = append(allGrpImgs, ExtractIsomorphisms(n, leafEqual)...)
	}

	return allGrpImgs
}

// isomorphic test the node is isomorphic
// a--b--c2--d0
//     \   \
//      \    d1--e
//       c2--d0
//         \
//           d1--e
// node 'b' is isomorphic, but 'c2' is not.
// while checking from top to down, we found subnodes of 'b' are equal, so we define node 'b' is isomorphic.
// we don't check whether all subnodes of 'b' are isomorphic.
func isomorphic(n *html.Node, leafEqual func(c, n *html.Node) bool) bool {
	if n == nil || n.FirstChild == nil {
		return false
	} else if n.FirstChild.NextSibling == nil {
		return isomorphic(n.FirstChild, leafEqual)
	}

	var next *html.Node
	for curr := n.FirstChild; curr != n.LastChild; curr = next {
		next = curr.NextSibling
		if !NodeEqual(curr, next, leafEqual) {
			return true
		}
	}

	return false
}

// NodeEqual check two node is equal by depth first
// please make sure c,n not nil
func NodeEqual(c, n *html.Node, leafEqual func(c, n *html.Node) bool) bool {
	if isLeaf(c) && isLeaf(n) {
		return leafEqual(c, n)
	}
	if countSubNode(c) != countSubNode(n) {
		return false
	}

	ccurr := c.FirstChild
	ncurr := n.FirstChild
	for ccurr != nil && ncurr != nil {
		if !NodeEqual(ccurr, ncurr, leafEqual) {
			return false
		}
		ccurr = ccurr.NextSibling
		ncurr = ncurr.NextSibling
	}

	return true
}

func isLeaf(n *html.Node) bool {
	if n == nil {
		return false
	}
	if n.FirstChild == nil {
		return true
	}
	return false
}

func countSubNode(n *html.Node) int {
	if n == nil {
		return 0
	}
	num := 0
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		num++
	}
	return num
}

// ExtractIsomorphicLeaf find leaves with isomorphic path
// b--c2--d0           b--c2--d0           n--d0
//  \   \               \                   \
//   \   d1--e    =>     c2--d0       =>      d0
//    c2--d0           b--c2--d1--e        n--e
//      \               \                   \
//       d1--e           c2--d1--e            e
func ExtractIsomorphicLeaf(root *html.Node, leafEqual func(c, n *html.Node) bool) []*html.Node {
	if !needExtract(root, leafEqual) {
		return []*html.Node{root}
	}

	return parallelFindLeaf(root)
}

// n is isomorphic, but subnodes of n maybe not
func needExtract(n *html.Node, leafEqual func(c, n *html.Node) bool) bool {
	b, node := singleBranch(n)
	if b {
		return false
	}

	for curr := node.FirstChild; curr != nil; curr = curr.NextSibling {
		if b, _ := singleBranch(curr); !b {
			return true
		}
	}

	// compare depth of branch
	var next *html.Node
	for curr := node.FirstChild; curr != n.LastChild; curr = next {
		next = curr.NextSibling
		if next == nil {
			// all branches have same depth
			return false
		}
		if !NodeEqual(curr, next, leafEqual) {
			return true
		}
	}

	return false
}

func parallelFindLeaf(n *html.Node) []*html.Node {
	for n.FirstChild.NextSibling == nil {
		n = n.FirstChild
	}
	var chanSlice []chan *html.Node
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		c := iterator(curr)
		chanSlice = append(chanSlice, c)
	}
	num := len(chanSlice)

	var res []*html.Node
LOOP:
	for {
		newRoot := &html.Node{Type: html.ElementNode, Data: "div"}
		for i := 0; i < num; i++ {
			node, ok := <-chanSlice[i]
			if !ok {
				break LOOP
			}
			newRoot.AppendChild(node)
		}
		res = append(res, newRoot)
	}
	return res
}

func iterator(n *html.Node) chan *html.Node {
	c := make(chan *html.Node)
	go func() {
		findLeaf(n, c)
		close(c)
	}()

	return c
}

func findLeaf(n *html.Node, c chan<- *html.Node) {
	if isLeaf(n) {
		node := &html.Node{
			Type:     n.Type,
			DataAtom: n.DataAtom,
			Data:     n.Data,
			Attr:     make([]html.Attribute, len(n.Attr)),
		}
		copy(node.Attr, n.Attr)
		c <- node
	} else if n == nil {
		return
	}

	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		findLeaf(curr, c)
	}
}

func singleBranch(n *html.Node) (bool, *html.Node) {
	if n == nil || n.FirstChild == nil {
		return true, n
	} else if n.FirstChild.NextSibling == nil {
		return singleBranch(n.FirstChild)
	}
	return false, n
}
