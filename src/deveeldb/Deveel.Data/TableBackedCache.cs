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
using System.Collections.Generic;

using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// Special type of a cache in a <see cref="TableDataConglomerate"/> that 
	/// is backed by a table in the database.
	/// </summary>
	/// <remarks>
	/// The purpose of this object is to provide efficient access to some 
	/// specific information in a table via a cache.
	/// <para>
	/// This object can be used, for instance, to provide cached access to the system
	/// privilege tables. The engine often performs identical types of priv
	/// queries on the database and it's desirable to cache the access to this
	/// table.
	/// </para>
	/// <para>
	/// This class provides the following services:
	/// <list type="number">
	/// <item>Allows for an instance of this object to be attached to a single
	/// <see cref="DatabaseConnection"/></item>
	/// <item>Listens for any changes that are committed to the table(s) and flushes the
	/// cache as neccessary.</item>
	/// </list>
	/// </para>
	/// <para>
	/// This object is designed to fit into the pure serializable transaction isolation 
	/// system that the system employs. This object will provide a view of the table as 
	/// it was when the transaction started. When the transaction commits (or rollsback) 
	/// the view is updated to the most current version. If a change is committed to the 
	/// tables this cache is backed by, the cache is only flushed when there are no open 
	/// transactions on the
	/// session.
	/// </para>
	/// </remarks>
	abstract class TableBackedCache {
		/// <summary>
		/// The table that this cache is backed by.
		/// </summary>
		private readonly TableName backedByTable;

		/// <summary>
		/// The list of added rows to the table above when a change 
		/// is committed.
		/// </summary>
		private readonly IList<int> addedList;

		/// <summary>
		/// The list of removed rows from the table above when a change 
		/// is committed.
		/// </summary>
		private readonly IList<int> removedList;

		/// <summary>
		/// Set to true when the backing DatabaseConnection has a transaction open.
		/// </summary>
		private bool transactionActive;

		protected TableBackedCache(TableName table) {
			backedByTable = table;

			addedList = new List<int>();
			removedList = new List<int>();
		}

		public bool IsTransactionActive {
			get { return transactionActive; }
		}

		/// <summary>
		/// Adds new row ids to the given list.
		/// </summary>
		/// <param name="from"></param>
		/// <param name="list"></param>
		private static void AddRowsToList(int[] from, IList<int> list) {
			if (from != null) {
				foreach (int i in from) {
					list.Add(i);
				}
			}
		}

		private void CommitModification(SimpleTransaction sender, CommitModificationEventArgs args) {
			TableName tableName = args.TableName;
			if (tableName.Equals(backedByTable)) {
				lock (removedList) {
					AddRowsToList(args.AddedRows, addedList);
					AddRowsToList(args.RemovedRows, removedList);
				}
			}			
		}

		/// <summary>
		/// Attaches this object to a conglomerate.
		/// </summary>
		/// <remarks>
		/// This applies the appropriate listeners to the tables.
		/// </remarks>
		internal void AttachTo(TableDataConglomerate conglomerate) {
			conglomerate.AddCommitModificationEventHandler(backedByTable, CommitModification);
		}

		/// <summary>
		/// Call to detach this object from a TableDataConglomerate.
		/// </summary>
		/// <param name="conglomerate"></param>
		internal void DetatchFrom(TableDataConglomerate conglomerate) {
			conglomerate.RemoveCommitModificationEventHandler(backedByTable, CommitModification);
		}

		/// <summary>
		/// Called from <see cref="DatabaseConnection"/> to notify 
		/// this object that a new transaction has been started.
		/// </summary>
		/// <remarks>
		/// When a transaction has started, any committed changes to the 
		/// table must NOT be immediately reflected in this cache. Only when 
		/// the transaction commits is there a possibility of the cache 
		/// information being incorrect.
		/// </remarks>
		internal void OnTransactionStarted() {
			transactionActive = true;
			InternalPurgeCache();
		}

		/// <summary>
		/// Called from <see cref="DatabaseConnection"/> to notify that object 
		/// that a transaction has closed.
		/// </summary>
		/// <remarks>
		/// When a transaction is closed, information in the cache may 
		/// be invalidated. For example, if rows 10 - 50 were delete then any
		/// information in the cache that touches this data must be flushed from the
		/// cache.
		/// </remarks>
		internal void OnTransactionFinished() {
			transactionActive = false;
			InternalPurgeCache();
		}

		/// <summary>
		/// Method which copies the <i>added</i> and <i>removed</i> rows and
		/// calls the <see cref="PurgeCache"/>.
		/// </summary>
		private void InternalPurgeCache() {
			// Make copies of the added_list and removed_list
			IList<int> add, remove;
			lock (removedList) {
				add = new List<int>(addedList);
				remove = new List<int>(removedList);
				// Clear the added and removed list
				addedList.Clear();
				removedList.Clear();
			}
			// Make changes to the cache
			PurgeCache(add, remove);
		}

		/// <summary>
		/// This method is called when the transaction starts and finishes and must
		/// purge the cache of all invalidated entries.
		/// </summary>
		/// <param name="addedRows"></param>
		/// <param name="removedRows"></param>
		/// <remarks>
		/// This method must <b>not</b> make any queries on the database. It must
		/// only, at the most, purge the cache of invalid entries.  A trivial
		/// implementation of this might completely clear the cache of all data if
		/// <paramref name="removedRows"/>.Count &gt; 0.
		/// </remarks>
		protected abstract void PurgeCache(IList<int> addedRows, IList<int> removedRows);
	}
}