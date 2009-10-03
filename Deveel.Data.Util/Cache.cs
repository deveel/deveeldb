// 
//  Cache.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Util {
	///<summary>
	/// Represents a cache of Objects.
	///</summary>
	/// <remarks>
	/// A <see cref="Cache"/> is similar to a <see cref="Hashtable"/>, in that you can 
	/// <see cref="Hashtable.Add">add</see> and <see cref="Hashtable.this">get</see> 
	/// objects from the container given some key. However a cache may remove objects 
	/// from the container when it becomes too full.
	/// <para>
	/// The cache scheme uses a doubly linked-list hashtable.  The most recently
	/// accessed objects are moved to the start of the list.  The end elements in
	/// the list are wiped if the cache becomes too full.
	/// </para>
	/// </remarks>
	public class Cache {
		/// <summary>
		/// The maximum number of DataCell objects that can be stored in 
		/// the cache at any one time.
		/// </summary>
		private readonly int max_cache_size;

		/// <summary>
		/// The current cache size.
		/// </summary>
		private int current_cache_size;

		/// <summary>
		/// The number of nodes that should be left available when the cache becomes
		/// too full and a clean up operation occurs.
		/// </summary>
		private readonly int wipe_to;

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

		/**
		 * The Constructors.  It takes a maximum size the cache can grow to, and the
		 * percentage of the cache that is wiped when it becomes too full.
		 */
		///<summary>
		///</summary>
		///<param name="hash_size"></param>
		///<param name="max_size"></param>
		///<param name="clean_percentage"></param>
		///<exception cref="Exception"></exception>
		public Cache(int hash_size, int max_size, int clean_percentage) {
			if (clean_percentage >= 85) {
				throw new Exception(
						  "Can't set to wipe more than 85% of the cache during clean.");
			}
			max_cache_size = max_size;
			current_cache_size = 0;
			wipe_to = max_size - ((clean_percentage * max_size) / 100);

			node_hash = new ListNode[hash_size];

			list_start = null;
			list_end = null;
		}

		///<summary>
		///</summary>
		///<param name="max_size"></param>
		///<param name="clean_percentage"></param>
		public Cache(int max_size, int clean_percentage)
			: this((max_size * 2) + 1, max_size, 20) {
		}

		///<summary>
		///</summary>
		///<param name="max_size"></param>
		public Cache(int max_size)
			: this(max_size, 20) {
		}

		///<summary>
		///</summary>
		public Cache()
			: this(50) {
		}

		/// <summary>
		/// </summary>
		[Obsolete("Deprecated", false)]
		protected int HashSize {
			get { return (int) (max_cache_size*2) + 1; }
		}


		/// <summary>
		/// This is called whenever at object is put into the cache.
		/// </summary>
		/// <remarks>
		/// This method should determine if the cache should be cleaned and 
		/// call the clean method if appropriate.
		/// </remarks>
		protected virtual void CheckClean() {
			// If we have reached maximum cache size, remove some elements from the
			// end of the list
			if (current_cache_size >= max_cache_size) {
				Clean();
			}
		}

		/// <summary>
		/// Checks if the clean-up method should clean up more elements from 
		/// the cache.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the clean-up method that periodically cleans 
		/// up the cache should clean up more elements from the cache, otherwise
		/// <b>false</b>.
		/// </returns>
		protected virtual bool WipeMoreNodes() {
			return (current_cache_size >= wipe_to);
		}

		/// <summary>
		/// Notifies that the given object has been wiped from the cache by the
		/// clean up procedure.
		/// </summary>
		/// <param name="ob">The node being wiped.</param>
		protected virtual void OnWipingNode(Object ob) {
		}

		/// <summary>
		/// Notifies that some statistical information about the hash map has
		/// updated.
		/// </summary>
		/// <param name="total_walks"></param>
		/// <param name="total_get_ops"></param>
		/// <remarks>
		/// This should be used to compile statistical information about
		/// the number of walks a <i>get</i> operation takes to retreive an 
		/// entry from the hash.
		/// <para>
		/// This method is called every 8192 gets.
		/// </para>
		/// </remarks>
		protected virtual void OnGetWalks(long total_walks, long total_get_ops) {
		}

		// ---------- Hashing methods ----------

		/**
		 * Some statistics about the hashing algorithm.
		 */
		private long total_gets = 0;
		private long get_total = 0;

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
					throw new ApplicationException(
							"ListNode with same key already in the hash - remove first.");
				}
			}

			// Stick it in the hash list.
			node.next_hash_entry = node_hash[index];
			node_hash[index] = node;

			return node;
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
		/// Clears the entire hashtable of all entries.
		/// </summary>
		private void ClearHash() {
			for (int i = node_hash.Length - 1; i >= 0; --i) {
				node_hash[i] = null;
			}
		}


		// ---------- Public cache methods ----------

		/// <summary>
		/// Gets the number of nodes that are currently being stored in the
		/// cache.
		/// </summary>
		public int NodeCount {
			get { return current_cache_size; }
		}

		/// <summary>
		/// Puts an object into the cache with the given key.
		/// </summary>
		/// <param name="key">The key used to store the object.</param>
		/// <param name="ob">The object to add to the cache.</param>
		public void Set(Object key, Object ob) {

			// Do we need to clean any cache elements out?
			CheckClean();

			// Check whether the given key is already in the Hashtable.

			ListNode node = GetFromHash(key);
			if (node == null) {

				node = CreateListNode();
				node.key = key;
				node.contents = ob;

				// Add node to top.
				node.next = list_start;
				node.previous = null;
				list_start = node;
				if (node.next == null) {
					list_end = node;
				} else {
					node.next.previous = node;
				}

				++current_cache_size;

				// Add node to key mapping
				PutIntoHash(node);

			} else {

				// If key already in Hashtable, all we need to do is set node with
				// the new contents and bring the node to the start of the list.

				node.contents = ob;
				BringToHead(node);

			}

		}

		/// <summary>
		/// Gets the value of the node with the given key within the cache.
		/// </summary>
		/// <param name="key">The key of the node to return the value of.</param>
		/// <returns>
		/// Returns the value of the node with the given key within the cache,
		/// or <b>null</b> if none was found.
		/// </returns>
		public Object Get(Object key) {
			ListNode node = GetFromHash(key);

			if (node != null) {
				// Bring node to start of list.
				//      BringToHead(node);

				return node.contents;
			}

			return null;
		}

		/// <summary>
		/// Removes a node for the given key from the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <remarks>
		/// This is useful for ensuring the cache does not contain out-dated 
		/// information.
		/// </remarks>
		/// <returns>
		/// Returns the value of the removed node or <b>null</b> if none was
		/// found for the given key.
		/// </returns>
		public Object Remove(Object key) {
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

				--current_cache_size;

				Object contents = node.contents;

				// Set internals to null to ensure objects get gc'd
				node.contents = null;
				node.key = null;

				return contents;
			}

			return null;
		}

		/// <summary>
		/// Clear the cache of all the entries.
		/// </summary>
		public void Clear() {
			if (current_cache_size != 0) {
				current_cache_size = 0;
				ClearHash();
			}
			list_start = null;
			list_end = null;
		}
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
		/// Cleans away some old elements in the cache.
		/// </summary>
		/// <remarks>
		/// This method walks from the end, back <i>wipe count</i> elements 
		/// putting each object on the recycle stack.
		/// </remarks>
		/// <returns>
		/// Returns the number entries that were cleaned.
		/// </returns>
		protected int Clean() {

			ListNode node = list_end;
			if (node == null) {
				return 0;
			}

			int actual_count = 0;
			while (node != null && WipeMoreNodes()) {
				OnWipingNode(node.contents);

				RemoveFromHash(node.key);
				// Help garbage collector with old objects
				node.contents = null;
				node.key = null;
				ListNode old_node = node;
				// Move to previous node
				node = node.previous;

				// Help the GC by clearing away the linked list nodes
				old_node.next = null;
				old_node.previous = null;

				--current_cache_size;
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


		// ---------- Inner classes ----------

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