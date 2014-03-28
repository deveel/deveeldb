// 
//  Copyright 2010-2011  Deveel
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

using Deveel.Data.Index;
using Deveel.Diagnostics;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// A simple implementation of <see cref="Transaction"/> that provides various 
	/// facilities for implementing a Transaction object on a number of 
	/// <see cref="MasterTableDataSource"/> tables.
	/// </summary>
	/// <remarks>
	/// The <see cref="Transaction"/> object is designed such that concurrent 
	/// modification can happen to the database via other transactions without 
	/// this view of the database being changed.
	/// <para>
	/// This object does not implement any transaction control mechanisms such 
	/// as <c>commit</c> or <c>rollback</c>. This object is most useful for 
	/// setting up a short-term minimal transaction for modifying or querying 
	/// some data in the database given on some view.
	/// </para>
	/// </remarks>
	public abstract class SimpleTransaction {
		/// <summary>
		/// The TransactionSystem context.
		/// </summary>
		private readonly TransactionSystem system;
		/// <summary>
		/// The list of tables that represent this transaction's view of the database.
		/// (MasterTableDataSource).
		/// </summary>
		private readonly List<MasterTableDataSource> visibleTables;
		/// <summary>
		/// An IIndexSet for each visible table from the above list.  These objects
		/// are used to represent index information for all tables. 
		/// (IIndexSet)
		/// </summary>
		private readonly IList<IIndexSet> tableIndices;

		/// <summary>
		/// A queue of MasterTableDataSource and IIndexSet objects that are pending to
		/// be cleaned up when this transaction is disposed.
		/// </summary>
		private List<object> cleanupQueue;

		/// <summary>
		/// A cache of tables that have been accessed via this transaction.  This is
		/// a map of table_name -> IMutableTableDataSource.
		/// </summary>
		private readonly Dictionary<TableName, IMutableTableDataSource> tableCache;

		/// <summary>
		/// A local cache for sequence values.
		/// </summary>
		private readonly Dictionary<TableName, long> sequenceValueCache;

		/// <summary>
		/// The SequenceManager for this abstract transaction.
		/// </summary>
		private readonly SequenceManager sequenceManager;

		/// <summary>
		/// If true, this is a read-only transaction and does not permit any type of
		/// modification to this vew of the database.
		/// </summary>
		private bool readOnly;

		private readonly VariablesManager variables;

		/// <summary>
		/// Constructs a new <see cref="SimpleTransaction"/>.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="sequenceManager"></param>
		internal SimpleTransaction(TransactionSystem system, SequenceManager sequenceManager) {
			this.system = system;

			visibleTables = new List<MasterTableDataSource>();
			tableIndices = new List<IIndexSet>();
			tableCache = new Dictionary<TableName, IMutableTableDataSource>();
			sequenceValueCache = new Dictionary<TableName, long>();

			this.sequenceManager = sequenceManager;

			variables = new VariablesManager();

			readOnly = false;
		}

		///<summary>
		/// Sets this transaction as read-only.
		///</summary>
		/// <remarks>
		/// A read-only transaction does not allow for the view to be 
		/// modified in any way.
		/// </remarks>
		public virtual void SetReadOnly() {
			readOnly = true;
		}

		/// <summary>
		/// Gets if the transaction is read-only.
		/// </summary>
		/// <remarks>
		/// A read only transaction does not allow for the view to be modified 
		/// in any way.
		/// </remarks>
		public virtual bool IsReadOnly {
			get { return readOnly; }
		}

		internal VariablesManager Variables {
			get { return variables; }
		}

		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> that this <see cref="SimpleTransaction"/> 
		/// is part of.
		/// </summary>
		public TransactionSystem System {
			get { return system; }
		}

		/// <summary>
		/// Returns a list of all visible tables.
		/// </summary>
		internal IList<MasterTableDataSource> VisibleTables {
			get { return visibleTables; }
		}

		internal Logger Logger {
			get { return System.Logger; }
		}

		/// <summary>
		/// Returns the number of visible tables being managed by this transaction.
		/// </summary>
		protected virtual int VisibleTableCount {
			get { return visibleTables.Count; }
		}

		/// <summary>
		/// Returns a <see cref="MasterTableDataSource"/> object representing 
		/// table 'n' in the set of tables visible in this transaction.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		internal virtual MasterTableDataSource GetVisibleTable(int n) {
			return visibleTables[n];
		}

		/// <summary>
		/// Searches through the list of tables visible within this transaction and
		/// returns the <see cref="MasterTableDataSource"/> object with the 
		/// given name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="ignoreCase"></param>
		/// <returns>
		/// Returns null if no visible table with the given name could be found.
		/// </returns>
		internal virtual MasterTableDataSource FindVisibleTable(TableName tableName, bool ignoreCase) {
			int size = visibleTables.Count;
			for (int i = 0; i < size; ++i) {
				MasterTableDataSource master = visibleTables[i];
				DataTableInfo tableInfo = master.TableInfo;
				if (ignoreCase) {
					if (tableInfo.TableName.EqualsIgnoreCase(tableName))
						return master;
				} else {
					// Not ignore case
					if (tableInfo.TableName.Equals(tableName))
						return master;
				}
			}
			return null;
		}

		/// <summary>
		/// Returns the IndexSet for the given <see cref="MasterTableDataSource"/>
		/// object that is visible in this transaction.
		/// </summary>
		/// <param name="table"></param>
		/// <returns></returns>
		internal IIndexSet GetIndexSetForTable(MasterTableDataSource table) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i] == table) {
					return tableIndices[i];
				}
			}
			throw new Exception("MasterTableDataSource not found in this transaction.");
		}

		/// <summary>
		/// Sets the <see cref="IIndexSet"/> for the given <see cref="MasterTableDataSource"/> 
		/// object in this transaction.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="indexSet"></param>
		internal void SetIndexSetForTable(MasterTableDataSource table, IIndexSet indexSet) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i] == table) {
					tableIndices[i] = indexSet;
					return;
				}
			}
			throw new Exception("MasterTableDataSource not found in this transaction.");
		}

		/// <summary>
		/// Returns true if the given table name is a dynamically generated 
		/// table and is not a table that is found in the table list defined 
		/// in this transaction object.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or from 
		/// an external data source)
		/// </remarks>
		/// <returns></returns>
		protected virtual bool IsDynamicTable(TableName table_name) {
			// By default, dynamic tables are not implemented.
			return false;
		}

		/// <summary>
		/// If this transaction implementation defines dynamic tables (tables 
		/// whose content is determined by some function), this should return 
		/// the table here as a <see cref="ITableDataSource"/> object.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the table is not defined an exception is generated.
		/// </exception>
		protected virtual ITableDataSource GetDynamicTable(TableName tableName) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + tableName + "' not found.");
		}

		/// <summary>
		/// Returns the <see cref="DataTableInfo"/> for a dynamic table defined 
		/// in this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		protected virtual DataTableInfo GetDynamicTableInfo(TableName tableName) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + tableName + "' not found.");
		}

		/// <summary>
		/// Returns a string type describing the type of the dynamic table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		protected virtual String GetDynamicTableType(TableName tableName) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + tableName + "' not found.");
		}

		/// <summary>
		/// Returns a list of all dynamic table names.
		/// </summary>
		/// <remarks>
		/// We can assume that the object returned here is static so the 
		/// content of this list should not be changed.
		/// <para>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </para>
		/// </remarks>
		/// <returns></returns>
		protected virtual TableName[] GetDynamicTables() {
			return new TableName[0];
		}

		// -----

		/// <summary>
		/// Returns a new <see cref="IMutableTableDataSource"/> for the view of the
		/// <see cref="MasterTableDataSource"/> at the start of this transaction.
		/// </summary>
		/// <param name="master"></param>
		/// <remarks>
		/// Note that this is called only once per table accessed in this transaction.
		/// </remarks>
		/// <returns></returns>
		internal abstract IMutableTableDataSource CreateMutableTableDataSourceAtCommit(MasterTableDataSource master);

		// -----

		/// <summary>
		/// Flushes the table cache and purges the cache of the entry for 
		/// the given table name.
		/// </summary>
		/// <param name="tableName"></param>
		protected virtual void FlushTableCache(TableName tableName) {
			tableCache.Remove(tableName);
		}

		/// <summary>
		/// Adds a <see cref="MasterTableDataSource"/> and <see cref="IIndexSet"/> 
		/// to this transaction view.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="indexSet"></param>
		internal void AddVisibleTable(MasterTableDataSource table, IIndexSet indexSet) {
			if (IsReadOnly)
				throw new Exception("Transaction is Read-only.");

			visibleTables.Add(table);
			tableIndices.Add(indexSet);
		}

		/// <summary>
		/// Removes a MasterTableDataSource (and its IndexSet) from this view 
		/// and puts the information on the cleanup queue.
		/// </summary>
		/// <param name="table"></param>
		internal void RemoveVisibleTable(MasterTableDataSource table) {
			if (IsReadOnly)
				throw new Exception("Transaction is Read-only.");

			int i = visibleTables.IndexOf(table);
			if (i != -1) {
				visibleTables.RemoveAt(i);
				IIndexSet indexSet = tableIndices[i];
				tableIndices.RemoveAt(i);
				if (cleanupQueue == null)
					cleanupQueue = new List<object>();

				cleanupQueue.Add(table);
				cleanupQueue.Add(indexSet);
				// Remove from the table cache
				TableName tableName = table.TableName;
				tableCache.Remove(tableName);
			}
		}

		/// <summary>
		/// Updates a MastertableDataSource (and its IndexSet) for this view.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="index_set"></param>
		/// <remarks>
		/// The existing IIndexSet/MasterTableDataSource for this is put on 
		/// the clean up queue.
		/// </remarks>
		internal void UpdateVisibleTable(MasterTableDataSource table, IIndexSet index_set) {
			if (IsReadOnly)
				throw new Exception("Transaction is Read-only.");

			RemoveVisibleTable(table);
			AddVisibleTable(table, index_set);
		}

		/// <summary>
		/// Disposes of all IndexSet objects currently accessed by the transaction.
		/// </summary>
		/// <remarks>
		/// This includes <see cref="IIndexSet"/> objects on tables that have been 
		/// dropped by operations on this transaction and are in the 'cleanup_queue' 
		/// object. Disposing of the <see cref="IIndexSet"/> is a common cleanup 
		/// practice and would typically be used at the end of a transaction.
		/// </remarks>
		protected void DisposeAllIndices() {
			// Dispose all the IIndexSet for each table
			try {
				foreach (IIndexSet tableIndex in tableIndices) {
					tableIndex.Dispose();
				}
			} catch (Exception e) {
				Logger.Error(this, e);
			}

			// Dispose all tables we dropped (they will be in the cleanup_queue.
			try {
				if (cleanupQueue != null) {
					for (int i = 0; i < cleanupQueue.Count; i += 2) {
						MasterTableDataSource master = (MasterTableDataSource)cleanupQueue[i];
						IIndexSet indexSet = (IIndexSet)cleanupQueue[i + 1];
						indexSet.Dispose();
					}
					cleanupQueue = null;
				}
			} catch (Exception e) {
				Logger.Error(this, e);
			}

		}


		// -----

		/// <summary>
		/// Returns a <see cref="ITableDataSource"/> object that represents the 
		/// table with the given name within this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This table is represented by an immutable interface.
		/// </remarks>
		/// <returns></returns>
		public ITableDataSource GetTableDataSource(TableName tableName) {
			return GetTable(tableName);
		}

		/// <summary>
		/// Returns a <see cref="ITableDataSource"/> object that represents 
		/// the table with the given name within this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// Any changes made to this table are only made within the context of 
		/// this transaction. This means if a row is added or removed, it is 
		/// not made perminant until the transaction is committed.
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="Exception">
		/// If the table does not exist.
		/// </exception>
		public ITableDataSource GetTable(TableName tableName) {
			// If table is in the cache, return it
			IMutableTableDataSource table;
			if (tableCache.TryGetValue(tableName, out table))
				return table;

			// Is it represented as a master table?
			MasterTableDataSource master = FindVisibleTable(tableName, false);

			// Not a master table, so see if it's a dynamic table instead,
			if (master == null) {
				// Is this a dynamic table?
				if (IsDynamicTable(tableName))
					return GetDynamicTable(tableName);
			} else {
				// Otherwise make a view of tha master table data source and write it in
				// the cache.
				table = CreateMutableTableDataSourceAtCommit(master);

				// Put table name in the cache
				tableCache[tableName] = table;
			}

			return table;
		}

		/// <summary>
		/// Returns a <see cref="ITableDataSource"/> object that represents 
		/// the table with the given name within this transaction, as
		/// a <see cref="IMutableTableDataSource"/>.
		/// </summary>
		/// <param name="tableName">The name of the table to return</param>
		/// <remarks>
		/// This method queries <see cref="GetTable"/> and enforces the inheritance
		/// of the table returned from <see cref="IMutableTableDataSource"/>,
		/// throwing an exception if this contact is not respected.
		/// <para>
		/// This method should be called only when mutations on the returned table
		/// are needed.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <seealso cref="GetTable"/>
		public IMutableTableDataSource GetMutableTable(TableName tableName) {
			// first get the table e want to return
			ITableDataSource table = GetTable(tableName);

			// we enforce the contract of mutable table
			if (!(table is IMutableTableDataSource))
				throw new InvalidOperationException("Table  '" + tableName + "' is not mutable.");

			// and we return it casting
			return table as IMutableTableDataSource;
		}

		/// <summary>
		/// Returns the <see cref="DataTableInfo"/> for the table with the given 
		/// name that is visible within this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns>
		/// Returns null if table name doesn't refer to a table that exists.
		/// </returns>
		public DataTableInfo GetTableInfo(TableName tableName) {
			// If this is a dynamic table then handle specially
			if (IsDynamicTable(tableName))
				return GetDynamicTableInfo(tableName);

			// Otherwise return from the pool of visible tables
			foreach (MasterTableDataSource master in visibleTables) {
				DataTableInfo tableInfo = master.TableInfo;
				if (tableInfo.TableName.Equals(tableName))
					return tableInfo;
			}
			return null;
		}

		/// <summary>
		/// Returns a list of table names that are visible within this transaction.
		/// </summary>
		/// <returns></returns>
		public TableName[] GetTables() {
			TableName[] internalTables = GetDynamicTables();

			int sz = visibleTables.Count;
			// The result list
			TableName[] tables = new TableName[sz + internalTables.Length];
			// Add the master tables
			for (int i = 0; i < sz; ++i) {
				MasterTableDataSource master = visibleTables[i];
				DataTableInfo tableInfo = master.TableInfo;
				tables[i] = new TableName(tableInfo.Schema, tableInfo.Name);
			}

			// Add any internal system tables to the list
			for (int i = 0; i < internalTables.Length; ++i) {
				tables[sz + i] = internalTables[i];
			}

			return tables;
		}

		/// <summary>
		/// Returns true if the database table object with the given name exists
		/// within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns></returns>
		public virtual bool TableExists(TableName table_name) {
			return IsDynamicTable(table_name) ||
				   RealTableExists(table_name);
		}

		/// <summary>
		/// Returns true if the table with the given name exists within this 
		/// transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This is different from 'tableExists' because it does not try to 
		/// resolve against dynamic tables, and is therefore useful for quickly
		/// checking if a system table exists or not.
		/// </remarks>
		/// <returns></returns>
		internal bool RealTableExists(TableName tableName) {
			return FindVisibleTable(tableName, false) != null;
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// For example, "aPP.CuSTOMer" may resolve to "default.Customer". If the 
		/// table name can not resolve to a valid identifier it returns the 
		/// input table name, therefore the actual presence of the table should 
		/// always be checked by calling <see cref="TableExists"/> after this 
		/// method returns.
		/// </remarks>
		/// <returns></returns>
		public virtual TableName TryResolveCase(TableName tableName) {
			// Is it a visable table (match case insensitive)
			MasterTableDataSource table = FindVisibleTable(tableName, true);
			if (table != null)
				return table.TableName;

			// Is it an internal table?
			string tschema = tableName.Schema;
			string tname = tableName.Name;
			TableName[] list = GetDynamicTables();
			foreach (TableName ctable in list) {
				if (String.Compare(ctable.Schema, tschema, true) == 0 &&
					String.Compare(ctable.Name, tname, true) == 0) {
					return ctable;
				}
			}

			// No matches so return the original object.
			return tableName;
		}

		/// <summary>
		/// Returns the type of the table object with the given name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns>
		/// If the table is a base table, this method returns "TABLE". If it is 
		/// a virtual table, it returns the type assigned to by the 
		/// InternalTableInfo interface.
		/// </returns>
		public String GetTableType(TableName tableName) {
			if (IsDynamicTable(tableName))
				return GetDynamicTableType(tableName);
			if (FindVisibleTable(tableName, false) != null)
				return "TABLE";

			// No table found so report the error.
			throw new Exception("No table '" + tableName + "' to report type for.");
		}

		/// <summary>
		/// Resolves the given string to a table name.
		/// </summary>
		/// <param name="currentSchema"></param>
		/// <param name="name"></param>
		/// <param name="caseInsensitive"></param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambiguous or the table object is not found.
		/// </exception>
		public TableName ResolveToTableName(string currentSchema, string name, bool caseInsensitive) {
			TableName tableName = TableName.Resolve(currentSchema, name);
			TableName[] tables = GetTables();
			TableName found = null;

			for (int i = 0; i < tables.Length; ++i) {
				bool match;
				if (caseInsensitive) {
					match = tables[i].EqualsIgnoreCase(tableName);
				} else {
					match = tables[i].Equals(tableName);
				}
				if (match) {
					if (found != null)
						throw new StatementException("Ambiguous reference: " + name);

					found = tables[i];
				}
			}

			if (found == null)
				throw new StatementException("Object not found: " + name);

			return found;
		}

		// ---------- Sequence management ----------

		/// <summary>
		/// Flushes the sequence cache.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This should be used whenever a sequence is changed.
		/// </remarks>
		internal void FlushSequenceManager(TableName name) {
			sequenceManager.FlushGenerator(name);
		}

		/// <summary>
		/// Requests of the sequence generator the next value from the sequence.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note</b> This does <b>not</b> check that the user owning this 
		/// connection has the correct privs to perform this operation.
		/// </remarks>
		/// <returns></returns>
		public long NextSequenceValue(TableName name) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for Read only transaction.");

			// Check: if null sequence manager then sequence ops not allowed.
			if (sequenceManager == null)
				throw new Exception("Sequence operations are not permitted.");

			SequenceManager seq = sequenceManager;
			long val = seq.NextValue(this, name);
			// No synchronized because a DatabaseConnection should be single threaded
			// only.
			sequenceValueCache[name] = val;
			return val;
		}

		/// <summary>
		/// Returns the sequence value for the given sequence generator that
		/// was last returned by a call to <see cref="NextSequenceValue"/>.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// If a value was not last returned by a call to 'nextSequenceValue' 
		/// then a statement exception is generated.
		/// <para>
		/// <b>Note</b> This does <b>not</b> check that the user owning this 
		/// connection has the correct privs to perform this operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public long LastSequenceValue(TableName name) {
			// No synchronized because a DatabaseConnection should be single threaded
			// only.
			long value;
			if (sequenceValueCache.TryGetValue(name, out value))
				return value;

			throw new StatementException("Current value for sequence generator " + name + " is not available.");
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note</b> This does <b>not</b> check that the user owning this 
		/// connection has the correct privs to perform this operation.
		/// </remarks>
		/// <exception cref="ApplicationException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public void SetSequenceValue(TableName name, long value) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for Read only transaction.");

			// Check: if null sequence manager then sequence ops not allowed.
			if (sequenceManager == null)
				throw new Exception("Sequence operations are not permitted.");

			SequenceManager seq = sequenceManager;
			seq.SetValue(this, name, value);

			sequenceValueCache[name] = value;
		}

		/// <summary>
		/// Returns the current unique id for the given table name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// Note that this is <b>not</b> a view of the ID, it is the actual ID 
		/// value at this time regardless of transaction.
		/// </remarks>
		/// <returns></returns>
		public long CurrentUniqueID(TableName table_name) {
			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master == null)
				throw new StatementException("Table with name '" + table_name + "' could not be found to retrieve unique id.");

			return master.CurrentUniqueId;
		}

		/// <summary>
		/// Atomically returns a unique id that can be used as a seed for a set
		/// of unique identifiers for a table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// Values returned by this method are guarenteed unique within this 
		/// table. This is true even across transactions.
		/// <para>
		/// <b>Note</b> This change can not be rolled back.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public long NextUniqueID(TableName tableName) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master == null)
				throw new StatementException("Table with name '" + tableName + "' could not be found to retrieve unique id.");

			return master.GetNextUniqueId();
		}

		/// <summary>
		/// Sets the unique id for the given table name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="uniqueId"></param>
		/// <remarks>
		/// This must only be called under very controlled situations, such as 
		/// when altering a table or when we need to fix sequence corruption.
		/// </remarks>
		public void SetUniqueID(TableName tableName, long uniqueId) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master == null)
				throw new StatementException("Table with name '" + tableName + "' could not be found to set unique id.");

			master.SetUniqueID(uniqueId);
		}
	}
}