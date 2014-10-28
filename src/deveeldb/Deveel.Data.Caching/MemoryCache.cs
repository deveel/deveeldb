// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Configuration;

namespace Deveel.Data.Caching {
	public class MemoryCache : Cache {
		public MemoryCache(int hashSize, int maxSize, int cleanPercentage)
			: base(maxSize, cleanPercentage) {
			nodeHash = new ListNode[hashSize];

			listStart = null;
			listEnd = null;
		}

		///<summary>
		///</summary>
		///<param name="maxSize"></param>
		///<param name="cleanPercentage"></param>
		public MemoryCache(int maxSize, int cleanPercentage)
			: this((maxSize * 2) + 1, maxSize, cleanPercentage) {
		}

		/// <summary>
		/// The array of ListNode objects arranged by hashing value.
		/// </summary>
		private ListNode[] nodeHash;

		/// <summary>
		/// A pointer to the start of the list.
		/// </summary>
		private ListNode listStart;

		/// <summary>
		/// A pointer to the end of the list.
		/// </summary>
		private ListNode listEnd;

		// Some statistics about the hashing algorithm.
		private long totalGets = 0;
		private long getTotal = 0;

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
			for (int i = nodeHash.Length - 1; i >= 0; --i) {
				nodeHash[i] = null;
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
			int index = (hash & 0x7FFFFFFF) % nodeHash.Length;
			ListNode prev = null;
			for (ListNode e = nodeHash[index]; e != null; e = e.NextHashEntry) {
				if (key.Equals(e.Key)) {
					// Found entry, so remove it baby!
					if (prev == null) {
						nodeHash[index] = e.NextHashEntry;
					} else {
						prev.NextHashEntry = e.NextHashEntry;
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
			int hash = node.Key.GetHashCode();
			int index = (hash & 0x7FFFFFFF) % nodeHash.Length;
			Object key = node.Key;
			for (ListNode e = nodeHash[index]; e != null; e = e.NextHashEntry) {
				if (key.Equals(e.Key)) {
					throw new ApplicationException("ListNode with same key already in the hash - remove first.");
				}
			}

			// Stick it in the hash list.
			node.NextHashEntry = nodeHash[index];
			nodeHash[index] = node;

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
			int index = (hash & 0x7FFFFFFF) % nodeHash.Length;
			int getCount = 1;

			for (ListNode e = nodeHash[index]; e != null; e = e.NextHashEntry) {
				if (key.Equals(e.Key)) {
					++totalGets;
					getTotal += getCount;

					// Every 8192 gets, call the 'OnGetWalks' method with the
					// statistical info.
					if ((totalGets & 0x01FFF) == 0) {
						try {
							OnGetWalks(getTotal, totalGets);
							// Reset stats if we overflow on an int
							if (getTotal > unchecked(65536 * 65536)) {
								getTotal = 0;
								totalGets = 0;
							}
						} catch (Exception) { /* ignore */ }
					}

					// Bring to head if get_count > 1
					if (getCount > 1) {
						BringToHead(e);
					}
					return e;
				}
				++getCount;
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
			if (listStart != node) {

				ListNode nextNode = node.Next;
				ListNode previousNode = node.Previous;

				node.Next = listStart;
				node.Previous = null;
				listStart = node;
				node.Next.Previous = node;

				if (nextNode != null) {
					nextNode.Previous = previousNode;
				} else {
					listEnd = previousNode;
				}
				previousNode.Next = nextNode;

			}
		}

		public override void Configure(IDbConfig config) {
			base.Configure(config);

			// Find a prime hash size depending on the size of the cache.
			int hashSize = ClosestPrime(MaxCacheSize / 55);
			nodeHash = new ListNode[hashSize];

			listStart = null;
			listEnd = null;
		}

		protected override bool SetObject(object key, object value) {
			// Check whether the given key is already in the Hashtable.

			ListNode node = GetFromHash(key);
			if (node == null) {

				node = CreateListNode();
				node.Key = key;
				node.Contents = value;

				// Add node to top.
				node.Next = listStart;
				node.Previous = null;
				listStart = node;
				if (node.Next == null) {
					listEnd = node;
				} else {
					node.Next.Previous = node;
				}

				// Add node to key mapping
				PutIntoHash(node);

				// this was added to the cache
				return true;
			}

			// If key already in Hashtable, all we need to do is set node with
			// the new contents and bring the node to the start of the list.

			node.Contents = value;
			BringToHead(node);

			return false;
		}

		protected override object GetObject(object key) {
			ListNode node = GetFromHash(key);

			if (node != null) {
				// Bring node to start of list.
				//      BringToHead(node);

				return node.Contents;
			}

			return null;
		}

		protected override object RemoveObject(object key) {
			object value = null;
			ListNode node = RemoveFromHash(key);

			if (node != null) {
				// If removed node at head.
				if (listStart == node) {
					listStart = node.Next;
					if (listStart != null) {
						listStart.Previous = null;
					} else {
						listEnd = null;
					}
				}
					// If removed node at end.
				else if (listEnd == node) {
					listEnd = node.Previous;
					if (listEnd != null) {
						listEnd.Next = null;
					} else {
						listStart = null;
					}
				} else {
					node.Previous.Next = node.Next;
					node.Next.Previous = node.Previous;
				}

				value = node.Contents;

				// Set internals to null to ensure objects get gc'd
				node.Contents = null;
				node.Key = null;
			}

			return value;
		}

		protected override int Clean() {
			ListNode node = listEnd;
			if (node == null) {
				return 0;
			}

			int actualCount = 0;
			while (node != null && WipeMoreNodes()) {
				object nkey = node.Key;
				object ncontents = node.Contents;

				OnWipingNode(ncontents);

				RemoveFromHash(nkey);
				// Help garbage collector with old objects
				node.Contents = null;
				node.Key = null;
				ListNode oldNode = node;
				// Move to previous node
				node = node.Previous;

				// Help the GC by clearing away the linked list nodes
				oldNode.Next = null;
				oldNode.Previous = null;

				OnObjectRemoved(nkey, ncontents);
				++actualCount;
			}

			if (node != null) {
				node.Next = null;
				listEnd = node;
			} else {
				listStart = null;
				listEnd = null;
			}

			return actualCount;
		}

		public override void Clear() {
			ClearHash();
			listStart = null;
			listEnd = null;
			base.Clear();
		}

		/// <summary>
		/// An element in the linked list structure.
		/// </summary>
		sealed class ListNode {
			// Links to the next and previous nodes. The ends of the list are 'null'
			public ListNode Next;
			public ListNode Previous;
			// The next node in the hash link on this hash value, or 'null' if last
			// hash entry.
			public ListNode NextHashEntry;

			// The key in the Hashtable for this object.
			public object Key;
			// The object contents for this element.
			public object Contents;
		}
	}
}