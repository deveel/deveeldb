using System;
using System.Collections;

namespace Deveel.Collections {
	[Serializable]
	public class TreeDictionary : ISortedDictionary, ICloneable {
		#region ctor
		public TreeDictionary() :
			this((IComparer)null) {
		}

		public TreeDictionary(IComparer comparer) {
			this.comparer = comparer;
			BuildTree(0);
		}

		public TreeDictionary(IDictionary dictionary) :
			this((IComparer)null) {
			AddDictionary(dictionary);
		}

		static TreeDictionary() {
			NullNode.left = NullNode;
			NullNode.parent = NullNode;
			NullNode.right = NullNode;
		}
		#endregion

		#region Fields
		private Node root;
		private int count;
		private ICollection keys;
		private ICollection values;
		private int modCount;
		private IComparer comparer;

		static readonly int Black = 1;
		static readonly int Red = -1;

		static readonly int KeysType = 1;
		static readonly int ValuesType = 2;
		static readonly int EntriesType = 3;

		static readonly Node NullNode = new Node(null, null, Black);
		#endregion

		#region Protected Methods
		public virtual IComparer Comparer {
			get { return comparer; }
		}

		public virtual object FirstKey {
			get {
				if (root == null)
					throw new NullReferenceException();
				return FirstNode.entry.Key;
			}
		}

		public virtual object LastKey {
			get {
				if (root == null)
					throw new NullReferenceException();
				return LastNode.entry.Key;
			}
		}

		public virtual object this[object key] {
			get { return GetNode(key).entry.Value; }
			set { GetNode(key).SetValue(value); }
		}

		public virtual ICollection Keys {
			get {
				if (keys == null)
					keys = new Collection(this, KeysType);
				return keys;
			}
		}

		public virtual ICollection Values {
			get {
				if (values == null)
					values = new Collection(this, ValuesType);
				return values;
			}
		}

		public bool IsFixedSize {
			get { return false; }
		}

		public bool IsReadOnly {
			get { return false; }
		}

		public virtual int Count {
			get { return count; }
		}

		private Node FirstNode {
			get {
				// Exploit fact that nil.left == nil.
				Node node = root;
				while (node.left != NullNode)
					node = node.left;
				return node;
			}
		}

		private Node LastNode {
			get {
				// Exploit fact that nil.right == nil.
				Node node = root;
				while (node.right != NullNode)
					node = node.right;
				return node;
			}
		}

		public bool IsSynchronized {
			get { return false; }
		}

		public object SyncRoot {
			get { return this; }
		}
		#endregion

		#region Private Methods
		private void BuildTree(int nodeCount) {
			if (nodeCount == 0) {
				root = NullNode;
				count = 0;
				return;
			}

			// We color every row of nodes black, except for the overflow nodes.
			// I believe that this is the optimal arrangement. We construct the tree
			// in place by temporarily linking each node to the next node in the row,
			// then updating those links to the children when working on the next row.

			// Make the root node.
			root = new Node(null, null, Black);
			count = nodeCount;
			Node row = root;
			int rowsize;

			// Fill each row that is completely full of nodes.
			for (rowsize = 2; rowsize + rowsize <= nodeCount; rowsize <<= 1) {
				Node tmpParent = row;
				Node last = null;
				for (int j = 0; j < rowsize; j += 2) {
					Node left = new Node(null, null, Black);
					Node right = new Node(null, null, Black);
					left.parent = tmpParent;
					left.right = right;
					right.parent = tmpParent;
					tmpParent.left = left;
					Node next = tmpParent.right;
					tmpParent.right = right;
					tmpParent = next;
					if (last != null)
						last.right = left;
					last = right;
				}
				row = row.left;
			}

			// Now do the partial final row in red.
			int overflow = count - rowsize;
			Node parent = row;
			int i;
			for (i = 0; i < overflow; i += 2) {
				Node left = new Node(null, null, Red);
				Node right = new Node(null, null, Red);
				left.parent = parent;
				right.parent = parent;
				parent.left = left;
				Node next = parent.right;
				parent.right = right;
				parent = next;
			}
			// Add a lone left node if necessary.
			if (i - overflow == 0) {
				Node left = new Node(null, null, Red);
				left.parent = parent;
				parent.left = left;
				parent = parent.right;
				left.parent.right = NullNode;
			}
			// Unlink the remaining nodes of the previous row.
			while (parent != NullNode) {
				Node next = parent.right;
				parent.right = NullNode;
				parent = next;
			}
		}

		private Node GetNode(object key) {
			Node current = root;
			while (current != NullNode) {
				int comparison = Compare(key, current.entry.Key);
				if (comparison > 0)
					current = current.right;
				else if (comparison < 0)
					current = current.left;
				else
					return current;
			}
			return current;
		}

		private Node GetHighestLessThan(object key) {
			if (key == NullNode)
				return LastNode;

			Node last = NullNode;
			Node current = root;
			int comparison = 0;

			while (current != NullNode) {
				last = current;
				comparison = Compare(key, current.entry.Key);
				if (comparison > 0)
					current = current.right;
				else if (comparison < 0)
					current = current.left;
				else // Exact match.
					return Predecessor(last);
			}
			return comparison <= 0 ? Predecessor(last) : last;
		}

		private Node GetLowestGreaterThan(object key, bool first) {
			if (key == NullNode)
				return first ? FirstNode : NullNode;

			Node last = NullNode;
			Node current = root;
			int comparison = 0;

			while (current != NullNode) {
				last = current;
				comparison = Compare(key, current.entry.Key);
				if (comparison > 0)
					current = current.right;
				else if (comparison < 0)
					current = current.left;
				else
					return current;
			}
			return comparison > 0 ? Successor(last) : last;
		}

		private void InsertFixup(Node n) {
			// Only need to rebalance when parent is a RED node, and while at least
			// 2 levels deep into the tree (ie: node has a grandparent). Remember
			// that nil.color == BLACK.
			while (n.parent.color == Red && n.parent.parent != NullNode) {
				if (n.parent == n.parent.parent.left) {
					Node uncle = n.parent.parent.right;
					// Uncle may be nil, in which case it is BLACK.
					if (uncle.color == Red) {
						// Case 1. Uncle is Red: Change colors of parent, uncle,
						// and grandparent, and move n to grandparent.
						n.parent.color = Black;
						uncle.color = Black;
						uncle.parent.color = Red;
						n = uncle.parent;
					} else {
						if (n == n.parent.right) {
							// Case 2. Uncle is BLACK and x is right child.
							// Move n to parent, and rotate n left.
							n = n.parent;
							RotateLeft(n);
						}
						// Case 3. Uncle is BLACK and x is left child.
						// Recolor parent, grandparent, and rotate grandparent right.
						n.parent.color = Black;
						n.parent.parent.color = Red;
						RotateRight(n.parent.parent);
					}
				} else {
					// Mirror image of above code.
					Node uncle = n.parent.parent.left;
					// Uncle may be nil, in which case it is BLACK.
					if (uncle.color == Red) {
						// Case 1. Uncle is RED: Change colors of parent, uncle,
						// and grandparent, and move n to grandparent.
						n.parent.color = Black;
						uncle.color = Black;
						uncle.parent.color = Red;
						n = uncle.parent;
					} else {
						if (n == n.parent.left) {
							// Case 2. Uncle is BLACK and x is left child.
							// Move n to parent, and rotate n right.
							n = n.parent;
							RotateRight(n);
						}
						// Case 3. Uncle is BLACK and x is right child.
						// Recolor parent, grandparent, and rotate grandparent left.
						n.parent.color = Black;
						n.parent.parent.color = Red;
						RotateLeft(n.parent.parent);
					}
				}
			}
			root.color = Black;
		}

		private void DeleteFixup(Node node, Node parent) {
			// if (parent == nil)
			//   throw new InternalError();
			// If a black node has been removed, we need to rebalance to avoid
			// violating the "same number of black nodes on any path" rule. If
			// node is red, we can simply recolor it black and all is well.
			while (node != root && node.color == Black) {
				if (node == parent.left) {
					// Rebalance left side.
					Node sibling = parent.right;
					// if (sibling == nil)
					//   throw new InternalError();
					if (sibling.color == Red) {
						// Case 1: Sibling is red.
						// Recolor sibling and parent, and rotate parent left.
						sibling.color = Black;
						parent.color = Red;
						RotateLeft(parent);
						sibling = parent.right;
					}

					if (sibling.left.color == Black && sibling.right.color == Black) {
						// Case 2: Sibling has no red children.
						// Recolor sibling, and move to parent.
						sibling.color = Red;
						node = parent;
						parent = parent.parent;
					} else {
						if (sibling.right.color == Black) {
							// Case 3: Sibling has red left child.
							// Recolor sibling and left child, rotate sibling right.
							sibling.left.color = Black;
							sibling.color = Red;
							RotateRight(sibling);
							sibling = parent.right;
						}
						// Case 4: Sibling has red right child. Recolor sibling,
						// right child, and parent, and rotate parent left.
						sibling.color = parent.color;
						parent.color = Black;
						sibling.right.color = Black;
						RotateLeft(parent);
						node = root; // Finished.
					}
				} else {
					// Symmetric "mirror" of left-side case.
					Node sibling = parent.left;
					// if (sibling == nil)
					//   throw new InternalError();
					if (sibling.color == Red) {
						// Case 1: Sibling is red.
						// Recolor sibling and parent, and rotate parent right.
						sibling.color = Black;
						parent.color = Red;
						RotateRight(parent);
						sibling = parent.left;
					}

					if (sibling.right.color == Black && sibling.left.color == Black) {
						// Case 2: Sibling has no red children.
						// Recolor sibling, and move to parent.
						sibling.color = Red;
						node = parent;
						parent = parent.parent;
					} else {
						if (sibling.left.color == Black) {
							// Case 3: Sibling has red right child.
							// Recolor sibling and right child, rotate sibling left.
							sibling.right.color = Black;
							sibling.color = Red;
							RotateLeft(sibling);
							sibling = parent.left;
						}
						// Case 4: Sibling has red left child. Recolor sibling,
						// left child, and parent, and rotate parent right.
						sibling.color = parent.color;
						parent.color = Black;
						sibling.left.color = Black;
						RotateRight(parent);
						node = root; // Finished.
					}
				}
			}
			node.color = Black;
		}

		private void RotateRight(Node node) {
			Node child = node.left;
			// if (node == NullNode || child == NullNode)
			//   throw new InternalError();

			// Establish node.left link.
			node.left = child.right;
			if (child.right != NullNode)
				child.right.parent = node;

			// Establish child->parent link.
			child.parent = node.parent;
			if (node.parent != NullNode) {
				if (node == node.parent.right)
					node.parent.right = child;
				else
					node.parent.left = child;
			} else
				root = child;

			// Link n and child.
			child.right = node;
			node.parent = child;
		}

		private void RotateLeft(Node node) {
			Node child = node.right;
			// if (node == NullNode || child == NullNode)
			//   throw new InternalError();

			// Establish node.right link.
			node.right = child.left;
			if (child.left != NullNode)
				child.left.parent = node;

			// Establish child->parent link.
			child.parent = node.parent;
			if (node.parent != NullNode) {
				if (node == node.parent.left)
					node.parent.left = child;
				else
					node.parent.right = child;
			} else
				root = child;

			// Link n and child.
			child.left = node;
			node.parent = child;
		}

		private int Compare(object o1, object o2) {
			return (comparer == null ? ((IComparable)o1).CompareTo(o2) : comparer.Compare(o1, o2));
		}

		private Node Predecessor(Node node) {
			if (node.left != NullNode) {
				node = node.left;
				while (node.right != NullNode)
					node = node.right;
				return node;
			}

			Node parent = node.parent;
			// Exploit fact that nil.left == nil and node is non-nil.
			while (node == parent.left) {
				node = parent;
				parent = node.parent;
			}
			return parent;
		}

		private Node Successor(Node node) {
			if (node.right != NullNode) {
				node = node.right;
				while (node.left != NullNode)
					node = node.left;
				return node;
			}

			Node parent = node.parent;
			// Exploit fact that nil.right == nil and node is non-nil.
			while (node == parent.right) {
				node = parent;
				parent = parent.parent;
			}
			return parent;
		}

		private void RemoveNode(Node node) {
			Node splice;
			Node child;

			modCount++;
			count--;

			// Find splice, the node at the position to actually remove from the tree.
			if (node.left == NullNode) {
				// Node to be deleted has 0 or 1 children.
				splice = node;
				child = node.right;
			} else if (node.right == NullNode) {
				// Node to be deleted has 1 child.
				splice = node;
				child = node.left;
			} else {
				// Node has 2 children. Splice is node's predecessor, and we swap
				// its contents into node.
				splice = node.left;
				while (splice.right != NullNode)
					splice = splice.right;
				child = splice.left;
				node.entry = splice.entry;
			}

			// Unlink splice from the tree.
			Node parent = splice.parent;
			if (child != NullNode)
				child.parent = parent;
			if (parent == NullNode) {
				// Special case for 0 or 1 node remaining.
				root = child;
				return;
			}
			if (splice == parent.left)
				parent.left = child;
			else
				parent.right = child;

			if (splice.color == Black)
				DeleteFixup(child, parent);
		}
		#endregion

		#region Public Methods
		public virtual void Clear() {
			if (count > 0) {
				modCount++;
				root = NullNode;
				count = 0;
			}
		}

		public virtual object Clone() {
			TreeDictionary copy = null;
			copy = (TreeDictionary)base.MemberwiseClone();
			copy.BuildTree(count);

			Node node = FirstNode;
			Node cnode = copy.FirstNode;

			while (node != NullNode) {
				cnode.entry = new DictionaryEntry(node.entry.Key, node.entry.Value);
				node = Successor(node);
				cnode = copy.Successor(cnode);
			}
			return copy;
		}

		public virtual bool ContainsKey(object key) {
			return GetNode(key) != NullNode;
		}

		public virtual bool ContainsValue(object value) {
			Node node = FirstNode;
			while (node != NullNode) {
				if (Equals(value, node.entry.Value))
					return true;
				node = Successor(node);
			}
			return false;
		}

		public virtual ISortedDictionary GetHeadDictionary(object endKey) {
			return new SubDictionary(this, NullNode, endKey);
		}

		public virtual ISortedDictionary GetSubDictionary(object startKey, object endKey) {
			return new SubDictionary(this, startKey, endKey);
		}

		public virtual ISortedDictionary TailDictionary(object startKey) {
			return new SubDictionary(this, startKey, NullNode);
		}

		public virtual void Add(object key, object value) {
			Node current = root;
			Node parent = NullNode;
			int comparison = 0;

			// Find new node's parent.
			while (current != NullNode) {
				parent = current;
				comparison = Compare(key, current.entry.Key);
				if (comparison > 0)
					current = current.right;
				else if (comparison < 0)
					current = current.left;
				else { // Key already in tree.
					current.SetValue(value);
					return;
				}
			}

			// Set up new node.
			Node n = new Node(key, value, Red);
			n.parent = parent;

			// Insert node in tree.
			modCount++;
			count++;
			if (parent == NullNode) {
				// Special case inserting into an empty tree.
				root = n;
				return;
			}
			if (comparison > 0)
				parent.right = n;
			else
				parent.left = n;

			// Rebalance after insert.
			InsertFixup(n);
		}

		public void AddDictionary(IDictionary dictionary) {
			IDictionaryEnumerator en = dictionary.GetEnumerator();
			while (en.MoveNext()) {
				Add(en.Key, en.Value);
			}
		}

		public bool Contains(object key) {
			return ContainsKey(key);
		}

		public void Remove(object key) {
			Node n = GetNode(key);
			if (n != NullNode)
				RemoveNode(n);
		}

		public IDictionaryEnumerator GetEnumerator() {
			return new TreeEnumerator(this, EntriesType);
		}

		public void CopyTo(Array array, int index) {
			throw new NotSupportedException();
		}
		#endregion

		#region IEnumerable Implementations
		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
		#endregion


		#region Node
		class Node {
			#region ctor
			public Node(object key, object value, int color) {
				entry = new DictionaryEntry(key, value);
				this.color = color;
			}
			#endregion

			#region Fields
			public int color;
			public DictionaryEntry entry;
			public Node left = NullNode;
			public Node right = NullNode;
			public Node parent = NullNode;
			#endregion

			#region Public Methods
			public void SetValue(object value) {
				this.entry = new DictionaryEntry(entry.Key, value);
			}
			#endregion
		}
		#endregion

		#region Collection
		class Collection : ICollection {
			#region ctor
			public Collection(TreeDictionary dictionary, int type) {
				this.dictionary = dictionary;
				this.type = type;
			}
			#endregion

			#region Fields
			private TreeDictionary dictionary;
			private int type;
			#endregion

			#region Properties
			public int Count {
				get { return dictionary.count; }
			}

			public bool IsSynchronized {
				get { return false; }
			}

			public object SyncRoot {
				get { return this; }
			}
			#endregion

			#region Public Methods
			public IEnumerator GetEnumerator() {
				return new TreeEnumerator(dictionary, type);
			}

			public void CopyTo(Array array, int index) {
				throw new NotSupportedException();
			}
			#endregion
		}
		#endregion

		#region TreeEnumerator
		class TreeEnumerator : IDictionaryEnumerator {
			#region ctor
			public TreeEnumerator(TreeDictionary dictionary, int type) {
				this.dictionary = dictionary;
				knownMod = dictionary.modCount;
				this.type = type;
				this.next = dictionary.FirstNode;
				this.max = NullNode;
			}

			public TreeEnumerator(TreeDictionary dictionary, int type, Node first, Node max) {
				this.dictionary = dictionary;
				knownMod = dictionary.modCount;
				this.type = type;
				this.next = first;
				this.max = max;
			}
			#endregion

			#region Fields
			private TreeDictionary dictionary;
			private int type;
			/** The number of modifications to the backing dictionary that we know about. */
			private int knownMod;
			/** The last Entry returned by a MoveNext() call. */
			private Node last;
			/** The next entry that should be returned by next(). */
			private Node next;
			/**
			 * The last node visible to this iterator. This is used when iterating
			 * on a SubMap.
			 */
			private Node max;
			#endregion

			#region Properties
			public object Current {
				get {
					if (knownMod != dictionary.modCount)
						throw new InvalidOperationException();
					if (next == max)
						throw new NullReferenceException();

					if (type == ValuesType)
						return last.entry.Value;
					else if (type == KeysType)
						return last.entry.Key;
					return last.entry;
				}
			}

			public DictionaryEntry Entry {
				get {
					if (knownMod != dictionary.modCount)
						throw new InvalidOperationException();
					if (next == max)
						throw new NullReferenceException();
					if (type != EntriesType)
						throw new InvalidOperationException();
					return last.entry;
				}
			}

			public object Key {
				get { return Entry.Key; }
			}

			public object Value {
				get { return Entry.Value; }
			}
			#endregion

			#region Public Methods
			public bool MoveNext() {
				if (knownMod != dictionary.modCount)
					throw new InvalidOperationException();

				last = next;
				next = dictionary.Successor(last);

				return next != max;
			}

			public void Reset() {
				//TODO:
			}
			#endregion
		}
		#endregion

		#region SubDictionary
		class SubDictionary : ISortedDictionary {
			#region ctor
			public SubDictionary(TreeDictionary dictionary, object minKey, object maxKey) {
				if (minKey != NullNode && maxKey != NullNode && dictionary.Compare(minKey, maxKey) > 0)
					throw new ArgumentException("fromKey > toKey");
				this.dictionary = dictionary;
				this.minKey = minKey;
				this.maxKey = maxKey;
			}
			#endregion

			#region Fields
			private TreeDictionary dictionary;
			/**
			 * The lower range of this view, inclusive, or nil for unbounded.
			 * Package visible for use by nested classes.
			 */
			private object minKey;

			/**
			 * The upper range of this view, exclusive, or nil for unbounded.
			 * Package visible for use by nested classes.
			 */
			private object maxKey;

			/**
			 * The cache for {@link #entrySet()}.
			 */
			private ArrayList entries;
			private ICollection keys;
			private ICollection values;
			#endregion

			#region Private Methods
			private bool KeyInRange(object key) {
				return ((minKey == NullNode || dictionary.Compare(key, minKey) >= 0) &&
					(maxKey == NullNode || dictionary.Compare(key, maxKey) < 0));
			}
			#endregion

			#region Properties
			public IComparer Comparer {
				get { return dictionary.comparer; }
			}

			public object FirstKey {
				get {
					Node node = dictionary.GetLowestGreaterThan(minKey, true);
					if (node == NullNode || !KeyInRange(node.entry.Key))
						throw new NullReferenceException();
					return node.entry.Key;
				}
			}

			public object LastKey {
				get {
					Node node = dictionary.GetHighestLessThan(maxKey);
					if (node == NullNode || !KeyInRange(node.entry.Key))
						throw new NullReferenceException();
					return node.entry.Key;
				}
			}

			public bool IsFixedSize {
				get { return false; }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public ICollection Keys {
				get {
					if (keys == null)
						keys = new Collection(this, KeysType);
					return keys;
				}
			}

			public ICollection Values {
				get {
					if (values == null)
						values = new Collection(this, ValuesType);
					return values;
				}
			}

			public object this[object key] {
				get {
					if (KeyInRange(key))
						return dictionary[key];
					return null;
				}
				set {
					if (!KeyInRange(key))
						throw new ArgumentException();
					dictionary[key] = value;
				}
			}

			public int Count {
				get {
					Node node = dictionary.GetLowestGreaterThan(minKey, true);
					Node max = dictionary.GetLowestGreaterThan(maxKey, false);
					int count = 0;
					while (node != max) {
						count++;
						node = dictionary.Successor(node);
					}
					return count;
				}
			}

			public bool IsSynchronized {
				get { return false; }
			}

			public object SyncRoot {
				get { return dictionary; }
			}
			#endregion

			#region IEnumerable Implementations
			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}
			#endregion

			#region Public Methods
			public ISortedDictionary GetHeadDictionary(object endKey) {
				if (!KeyInRange(endKey))
					throw new ArgumentException("key outside range");
				return new SubDictionary(dictionary, minKey, endKey);
			}

			public ISortedDictionary GetSubDictionary(object startKey, object endKey) {
				if (!KeyInRange(startKey) || !KeyInRange(endKey))
					throw new ArgumentException("key outside range");
				return new SubDictionary(dictionary, startKey, endKey);
			}

			public ISortedDictionary TailDictionary(object startKey) {
				if (!KeyInRange(startKey))
					throw new ArgumentException("key outside range");
				return new SubDictionary(dictionary, startKey, maxKey);
			}

			public void Add(object key, object value) {
				if (!KeyInRange(key))
					throw new ArgumentException("Key outside range");
				dictionary.Add(key, value);
			}

			public void Clear() {
				Node next = dictionary.GetLowestGreaterThan(minKey, true);
				Node max = dictionary.GetLowestGreaterThan(maxKey, false);
				while (next != max) {
					Node current = next;
					next = dictionary.Successor(current);
					dictionary.RemoveNode(current);
				}
			}

			public bool Contains(object key) {
				return KeyInRange(key) && dictionary.ContainsKey(key);
			}

			public bool ContainsKey(object key) {
				return Contains(key);
			}

			public bool ContainsValue(object value) {
				throw new NotSupportedException();
			}

			public IDictionaryEnumerator GetEnumerator() {
				Node first = dictionary.GetLowestGreaterThan(minKey, true);
				Node max = dictionary.GetLowestGreaterThan(maxKey, false);
				return new TreeEnumerator(dictionary, EntriesType, first, max);
			}

			public void Remove(object key) {
				if (KeyInRange(key))
					dictionary.Remove(key);
			}

			public void CopyTo(Array array, int index) {
				throw new NotSupportedException();
			}
			#endregion

			#region Collection
			class Collection : ICollection {
				#region ctor
				public Collection(SubDictionary dictionary, int type) {
					this.dictionary = dictionary;
					this.type = type;
				}
				#endregion

				#region Fields
				private int type;
				private SubDictionary dictionary;
				#endregion

				#region Properties
				public int Count {
					get { return dictionary.Count; }
				}

				public bool IsSynchronized {
					get { return false; }
				}

				public object SyncRoot {
					get { return dictionary; }
				}
				#endregion

				#region Public Methods
				public void CopyTo(Array array, int index) {
					throw new NotSupportedException();
				}

				public IEnumerator GetEnumerator() {
					Node first = dictionary.dictionary.GetLowestGreaterThan(dictionary.minKey, true);
					Node max = dictionary.dictionary.GetLowestGreaterThan(dictionary.maxKey, false);
					return new TreeEnumerator(dictionary.dictionary, type, first, max);
				}
				#endregion
			}
			#endregion
		}
		#endregion
	}
}