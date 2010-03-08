// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

namespace Deveel.Data.Caching {
	internal class MemoryCache : Cache {
		public MemoryCache(int hash_size, int max_size, int clean_percentage)
			: base(max_size, clean_percentage) {
			node_hash = new ListNode[hash_size];

			list_start = null;
			list_end = null;
		}

		///<summary>
		///</summary>
		///<param name="max_size"></param>
		///<param name="clean_percentage"></param>
		public MemoryCache(int max_size, int clean_percentage)
			: this((max_size * 2) + 1, max_size, clean_percentage) {
		}

		/// <summary>
		/// The array of ListNode objects arranged by hashing value.
		/// </summary>
		private readonly ListNode[] node_hash;

		/// <summary>
		/// A pointer to the start of the list.
		/// </summary>
		private ListNode list_start;

		/// <summary>
		/// A pointer to the end of the list.
		/// </summary>
		private ListNode list_end;

		// Some statistics about the hashing algorithm.
		private long total_gets = 0;
		private long get_total = 0;

		/// <summary>
		/// Creates a new ListNode.
		/// </summary>
		/// <remarks>
		/// If there is a free ListNode on the 'recycled_nodes' then it obtains 
		/// one from there, else it creates a new blank one.
		/// </remarks>
		/// <returns></returns>
		private ListNode CreateListNode() {
			return new ListNode();
		}

		/// <summary>
		/// Clears the entire hashtable of all entries.
		/// </summary>
		private void ClearHash() {
			for (int i = node_hash.Length - 1; i >= 0; --i) {
				node_hash[i] = null;
			}
		}

		/// <summary>
		/// Removes the given node from the hash table.
		/// </summary>
		/// <param name="key"></param>
		/// <returns>
		/// Returns the entry removed from the hash table or <b>null</b> if 
		/// none was found for the given key.
		/// </returns>
		private ListNode RemoveFromHash(Object key) {
			// Makes sure the key is not already in the HashMap.
			int hash = key.GetHashCode();
			int index = (hash & 0x7FFFFFFF) % node_hash.Length;
			ListNode prev = null;
			for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
				if (key.Equals(e.key)) {
					// Found entry, so remove it baby!
					if (prev == null) {
						node_hash[index] = e.next_hash_entry;
					} else {
						prev.next_hash_entry = e.next_hash_entry;
					}
					return e;
				}
				prev = e;
			}

			// Not found so return 'null'
			return null;
		}

		/// <summary>
		/// Puts the node with the given key into the hash table.
		/// </summary>
		/// <param name="node"></param>
		/// <returns>
		/// Returns the list node added.
		/// </returns>
		private ListNode PutIntoHash(ListNode node) {
			// Makes sure the key is not already in the HashMap.
			int hash = node.key.GetHashCode();
			int index = (hash & 0x7FFFFFFF) % node_hash.Length;
			Object key = node.key;
			for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
				if (key.Equals(e.key)) {
					throw new ApplicationException("ListNode with same key already in the hash - remove first.");
				}
			}

			// Stick it in the hash list.
			node.next_hash_entry = node_hash[index];
			node_hash[index] = node;

			return node;
		}

		/// <summary>
		/// Gets the node with the given key in the hash table.
		/// </summary>
		/// <param name="key"></param>
		/// <returns>
		/// Returns the node with the given key in the hash table or <b>null</b>
		/// if none value was found.
		/// </returns>
		private ListNode GetFromHash(Object key) {
			int hash = key.GetHashCode();
			int index = (hash & 0x7FFFFFFF) % node_hash.Length;
			int get_count = 1;

			for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
				if (key.Equals(e.key)) {
					++total_gets;
					get_total += get_count;

					// Every 8192 gets, call the 'OnGetWalks' method with the
					// statistical info.
					if ((total_gets & 0x01FFF) == 0) {
						try {
							OnGetWalks(get_total, total_gets);
							// Reset stats if we overflow on an int
							if (get_total > unchecked(65536 * 65536)) {
								get_total = 0;
								total_gets = 0;
							}
						} catch (Exception) { /* ignore */ }
					}

					// Bring to head if get_count > 1
					if (get_count > 1) {
						BringToHead(e);
					}
					return e;
				}
				++get_count;
			}
			return null;
		}

		/// <summary>
		/// Brings the given node to the start of the list.
		/// </summary>
		/// <param name="node">The node to move up.</param>
		/// <remarks>
		/// Only nodes at the end of the list are cleaned.
		/// </remarks>
		private void BringToHead(ListNode node) {
			if (list_start != node) {

				ListNode next_node = node.next;
				ListNode previous_node = node.previous;

				node.next = list_start;
				node.previous = null;
				list_start = node;
				node.next.previous = node;

				if (next_node != null) {
					next_node.previous = previous_node;
				} else {
					list_end = previous_node;
				}
				previous_node.next = next_node;

			}
		}

		protected override bool SetObject(object key, object value) {
			// Check whether the given key is already in the Hashtable.

			ListNode node = GetFromHash(key);
			if (node == null) {

				node = CreateListNode();
				node.key = key;
				node.contents = value;

				// Add node to top.
				node.next = list_start;
				node.previous = null;
				list_start = node;
				if (node.next == null) {
					list_end = node;
				} else {
					node.next.previous = node;
				}

				// Add node to key mapping
				PutIntoHash(node);

				// this was added to the cache
				return true;
			} else {
				// If key already in Hashtable, all we need to do is set node with
				// the new contents and bring the node to the start of the list.

				node.contents = value;
				BringToHead(node);

				return false;
			}
		}

		protected override object GetObject(object key) {
			ListNode node = GetFromHash(key);

			if (node != null) {
				// Bring node to start of list.
				//      BringToHead(node);

				return node.contents;
			}

			return null;
		}

		protected override object RemoveObject(object key) {
			object value = null;
			ListNode node = RemoveFromHash(key);

			if (node != null) {
				// If removed node at head.
				if (list_start == node) {
					list_start = node.next;
					if (list_start != null) {
						list_start.previous = null;
					} else {
						list_end = null;
					}
				}
					// If removed node at end.
				else if (list_end == node) {
					list_end = node.previous;
					if (list_end != null) {
						list_end.next = null;
					} else {
						list_start = null;
					}
				} else {
					node.previous.next = node.next;
					node.next.previous = node.previous;
				}

				value = node.contents;

				// Set internals to null to ensure objects get gc'd
				node.contents = null;
				node.key = null;
			}

			return value;
		}

		protected override int Clean() {
			ListNode node = list_end;
			if (node == null) {
				return 0;
			}

			int actual_count = 0;
			while (node != null && WipeMoreNodes()) {
				object nkey = node.key;
				object ncontents = node.contents;

				OnWipingNode(ncontents);

				RemoveFromHash(nkey);
				// Help garbage collector with old objects
				node.contents = null;
				node.key = null;
				ListNode old_node = node;
				// Move to previous node
				node = node.previous;

				// Help the GC by clearing away the linked list nodes
				old_node.next = null;
				old_node.previous = null;

				OnObjectRemoved(nkey, ncontents);
				++actual_count;
			}

			if (node != null) {
				node.next = null;
				list_end = node;
			} else {
				list_start = null;
				list_end = null;
			}

			return actual_count;
		}

		public override void Clear() {
			ClearHash();
			list_start = null;
			list_end = null;
			base.Clear();
		}

		/// <summary>
		/// An element in the linked list structure.
		/// </summary>
		sealed class ListNode {
			// Links to the next and previous nodes. The ends of the list are 'null'
			internal ListNode next;
			internal ListNode previous;
			// The next node in the hash link on this hash value, or 'null' if last
			// hash entry.
			internal ListNode next_hash_entry;

			// The key in the Hashtable for this object.
			internal Object key;
			// The object contents for this element.
			internal Object contents;
		}
	}
}