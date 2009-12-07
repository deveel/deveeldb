//  
//  SimpleTransaction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Diagnostics;

namespace Deveel.Data {
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
		private readonly ArrayList visible_tables;
		/// <summary>
		/// An IIndexSet for each visible table from the above list.  These objects
		/// are used to represent index information for all tables. 
		/// (IIndexSet)
		/// </summary>
		private readonly ArrayList table_indices;

		/// <summary>
		/// A queue of MasterTableDataSource and IIndexSet objects that are pending to
		/// be cleaned up when this transaction is disposed.
		/// </summary>
		private ArrayList cleanup_queue;

		/// <summary>
		/// A cache of tables that have been accessed via this transaction.  This is
		/// a map of table_name -> IMutableTableDataSource.
		/// </summary>
		private readonly Hashtable table_cache;

		/// <summary>
		/// A local cache for sequence values.
		/// </summary>
		private readonly Hashtable sequence_value_cache;

		/// <summary>
		/// The SequenceManager for this abstract transaction.
		/// </summary>
		private readonly SequenceManager sequence_manager;

		/// <summary>
		/// If true, this is a read-only transaction and does not permit any type of
		/// modification to this vew of the database.
		/// </summary>
		private bool read_only;

		private VariablesManager variables;


		/**
		 * Constructs the AbstractTransaction.  SequenceManager may be null in which
		 * case sequence generator operations are not permitted.
		 */
		internal SimpleTransaction(TransactionSystem system, SequenceManager sequence_manager) {
			this.system = system;

			visible_tables = new ArrayList();
			table_indices = new ArrayList();
			table_cache = new Hashtable();
			sequence_value_cache = new Hashtable();

			this.sequence_manager = sequence_manager;

			variables = new VariablesManager();

			this.read_only = false;
		}

		///<summary>
		/// Sets this transaction as read-only.
		///</summary>
		/// <remarks>
		/// A read-only transaction does not allow for the view to be 
		/// modified in any way.
		/// </remarks>
		public virtual void SetReadOnly() {
			read_only = true;
		}

		/// <summary>
		/// Gets if the transaction is read-only.
		/// </summary>
		/// <remarks>
		/// A read only transaction does not allow for the view to be modified 
		/// in any way.
		/// </remarks>
		public virtual bool IsReadOnly {
			get { return read_only; }
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
		protected ArrayList VisibleTables {
			get { return visible_tables; }
		}

		internal IDebugLogger Debug {
			get { return System.Debug; }
		}

		/// <summary>
		/// Returns the number of visible tables being managed by this transaction.
		/// </summary>
		protected virtual int VisibleTableCount {
			get { return visible_tables.Count; }
		}

		/// <summary>
		/// Returns a <see cref="MasterTableDataSource"/> object representing 
		/// table 'n' in the set of tables visible in this transaction.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		internal virtual MasterTableDataSource GetVisibleTable(int n) {
			return (MasterTableDataSource)visible_tables[n];
		}

		/// <summary>
		/// Searches through the list of tables visible within this transaction and
		/// returns the <see cref="MasterTableDataSource"/> object with the 
		/// given name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="ignore_case"></param>
		/// <returns>
		/// Returns null if no visible table with the given name could be found.
		/// </returns>
		internal virtual MasterTableDataSource FindVisibleTable(TableName table_name, bool ignore_case) {
			int size = visible_tables.Count;
			for (int i = 0; i < size; ++i) {
				MasterTableDataSource master =
										  (MasterTableDataSource)visible_tables[i];
				DataTableDef table_def = master.DataTableDef;
				if (ignore_case) {
					if (table_def.TableName.EqualsIgnoreCase(table_name)) {
						return master;
					}
				} else {
					// Not ignore case
					if (table_def.TableName.Equals(table_name)) {
						return master;
					}
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
			int sz = table_indices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visible_tables[i] == table) {
					return (IIndexSet)table_indices[i];
				}
			}
			throw new Exception(
							"MasterTableDataSource not found in this transaction.");
		}

		/// <summary>
		/// Sets the <see cref="IIndexSet"/> for the given <see cref="MasterTableDataSource"/> 
		/// object in this transaction.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="index_set"></param>
		internal void SetIndexSetForTable(MasterTableDataSource table, IIndexSet index_set) {
			int sz = table_indices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visible_tables[i] == table) {
					table_indices[i] = index_set;
					return;
				}
			}
			throw new Exception(
							"MasterTableDataSource not found in this transaction.");
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
		/// the table here as a <see cref="IMutableTableDataSource"/> object.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the table is not defined an exception is generated.
		/// </exception>
		protected virtual IMutableTableDataSource GetDynamicTable(TableName table_name) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + table_name + "' not found.");
		}

		/// <summary>
		/// Returns the <see cref="DataTableDef"/> for a dynamic table defined 
		/// in this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		protected virtual DataTableDef GetDynamicDataTableDef(TableName table_name) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + table_name + "' not found.");
		}

		/// <summary>
		/// Returns a string type describing the type of the dynamic table.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// It is intended this is implemented by derived classes to handle 
		/// dynamically generated tables (tables based on some function or 
		/// from an external data source)
		/// </remarks>
		/// <returns></returns>
		protected virtual String GetDynamicTableType(TableName table_name) {
			// By default, dynamic tables are not implemented.
			throw new StatementException("Table '" + table_name + "' not found.");
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
		/// <param name="table_name"></param>
		protected virtual void FlushTableCache(TableName table_name) {
			table_cache.Remove(table_name);
		}

		/// <summary>
		/// Adds a <see cref="MasterTableDataSource"/> and <see cref="IIndexSet"/> 
		/// to this transaction view.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="index_set"></param>
		internal void AddVisibleTable(MasterTableDataSource table, IIndexSet index_set) {
			if (IsReadOnly) {
				throw new Exception("Transaction is Read-only.");
			}

			visible_tables.Add(table);
			table_indices.Add(index_set);
		}

		/// <summary>
		/// Removes a MasterTableDataSource (and its IndexSet) from this view 
		/// and puts the information on the cleanup queue.
		/// </summary>
		/// <param name="table"></param>
		internal void RemoveVisibleTable(MasterTableDataSource table) {
			if (IsReadOnly) {
				throw new Exception("Transaction is Read-only.");
			}

			int i = visible_tables.IndexOf(table);
			if (i != -1) {
				visible_tables.RemoveAt(i);
				IIndexSet index_set = (IIndexSet)table_indices[i];
				table_indices.RemoveAt(i);
				if (cleanup_queue == null) {
					cleanup_queue = new ArrayList();
				}
				cleanup_queue.Add(table);
				cleanup_queue.Add(index_set);
				// Remove from the table cache
				TableName table_name = table.TableName;
				table_cache.Remove(table_name);
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
			if (IsReadOnly) {
				throw new Exception("Transaction is Read-only.");
			}

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
				for (int i = 0; i < table_indices.Count; ++i) {
					((IIndexSet)table_indices[i]).Dispose();
				}
			} catch (Exception e) {
				Debug.WriteException(e);
			}

			// Dispose all tables we dropped (they will be in the cleanup_queue.
			try {
				if (cleanup_queue != null) {
					for (int i = 0; i < cleanup_queue.Count; i += 2) {
						MasterTableDataSource master =
											  (MasterTableDataSource)cleanup_queue[i];
						IIndexSet index_set = (IIndexSet)cleanup_queue[i + 1];
						index_set.Dispose();
					}
					cleanup_queue = null;
				}
			} catch (Exception e) {
				Debug.WriteException(e);
			}

		}


		// -----

		/// <summary>
		/// Returns a <see cref="ITableDataSource"/> object that represents the 
		/// table with the given name within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This table is represented by an immutable interface.
		/// </remarks>
		/// <returns></returns>
		public ITableDataSource GetTableDataSource(TableName table_name) {
			return GetTable(table_name);
		}

		/// <summary>
		/// Returns a <see cref="IMutableTableDataSource"/> object that represents 
		/// the table with the given name within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// Any changes made to this table are only made within the context of 
		/// this transaction. This means if a row is added or removed, it is 
		/// not made perminant until the transaction is committed.
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="Exception">
		/// If the table does not exist.
		/// </exception>
		public IMutableTableDataSource GetTable(TableName table_name) {

			// If table is in the cache, return it
			IMutableTableDataSource table =
								 (IMutableTableDataSource)table_cache[table_name];
			if (table != null) {
				return table;
			}

			// Is it represented as a master table?
			MasterTableDataSource master = FindVisibleTable(table_name, false);

			// Not a master table, so see if it's a dynamic table instead,
			if (master == null) {
				// Is this a dynamic table?
				if (IsDynamicTable(table_name)) {
					return GetDynamicTable(table_name);
				}
			} else {
				// Otherwise make a view of tha master table data source and write it in
				// the cache.
				table = CreateMutableTableDataSourceAtCommit(master);

				// Put table name in the cache
				table_cache[table_name] = table;
			}

			return table;

		}

		/// <summary>
		/// Returns the <see cref="DataTableDef"/> for the table with the given 
		/// name that is visible within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns>
		/// Returns null if table name doesn't refer to a table that exists.
		/// </returns>
		public DataTableDef GetDataTableDef(TableName table_name) {
			// If this is a dynamic table then handle specially
			if (IsDynamicTable(table_name)) {
				return GetDynamicDataTableDef(table_name);
			} else {
				// Otherwise return from the pool of visible tables
				int sz = visible_tables.Count;
				for (int i = 0; i < sz; ++i) {
					MasterTableDataSource master =
											 (MasterTableDataSource)visible_tables[i];
					DataTableDef table_def = master.DataTableDef;
					if (table_def.TableName.Equals(table_name)) {
						return table_def;
					}
				}
				return null;
			}
		}

		/// <summary>
		/// Returns a list of table names that are visible within this transaction.
		/// </summary>
		/// <returns></returns>
		public TableName[] GetTables() {
			TableName[] internal_tables = GetDynamicTables();

			int sz = visible_tables.Count;
			// The result list
			TableName[] tables = new TableName[sz + internal_tables.Length];
			// Add the master tables
			for (int i = 0; i < sz; ++i) {
				MasterTableDataSource master =
										 (MasterTableDataSource)visible_tables[i];
				DataTableDef table_def = master.DataTableDef;
				tables[i] = new TableName(table_def.Schema, table_def.Name);
			}

			// Add any internal system tables to the list
			for (int i = 0; i < internal_tables.Length; ++i) {
				tables[sz + i] = internal_tables[i];
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
		/// <param name="table_name"></param>
		/// <remarks>
		/// This is different from 'tableExists' because it does not try to 
		/// resolve against dynamic tables, and is therefore useful for quickly
		/// checking if a system table exists or not.
		/// </remarks>
		/// <returns></returns>
		internal bool RealTableExists(TableName table_name) {
			return FindVisibleTable(table_name, false) != null;
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// For example, "aPP.CuSTOMer" may resolve to "default.Customer". If the 
		/// table name can not resolve to a valid identifier it returns the 
		/// input table name, therefore the actual presence of the table should 
		/// always be checked by calling <see cref="TableExists"/> after this 
		/// method returns.
		/// </remarks>
		/// <returns></returns>
		public virtual TableName TryResolveCase(TableName table_name) {
			// Is it a visable table (match case insensitive)
			MasterTableDataSource table = FindVisibleTable(table_name, true);
			if (table != null) {
				return table.TableName;
			}
			// Is it an internal table?
			String tschema = table_name.Schema;
			String tname = table_name.Name;
			TableName[] list = GetDynamicTables();
			for (int i = 0; i < list.Length; ++i) {
				TableName ctable = list[i];
				if (String.Compare(ctable.Schema, tschema, true) == 0 &&
					String.Compare(ctable.Name, tname, true) == 0) {
					return ctable;
				}
			}

			// No matches so return the original object.
			return table_name;
		}

		/// <summary>
		/// Returns the type of the table object with the given name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns>
		/// If the table is a base table, this method returns "TABLE". If it is 
		/// a virtual table, it returns the type assigned to by the 
		/// InternalTableInfo interface.
		/// </returns>
		public String GetTableType(TableName table_name) {
			if (IsDynamicTable(table_name)) {
				return GetDynamicTableType(table_name);
			} else if (FindVisibleTable(table_name, false) != null) {
				return "TABLE";
			}
			// No table found so report the error.
			throw new Exception("No table '" + table_name +
									   "' to report type for.");
		}

		/// <summary>
		/// Resolves the given string to a table name.
		/// </summary>
		/// <param name="current_schema"></param>
		/// <param name="name"></param>
		/// <param name="case_insensitive"></param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambiguous or the table object is not found.
		/// </exception>
		public TableName ResolveToTableName(String current_schema, String name, bool case_insensitive) {
			TableName table_name = TableName.Resolve(current_schema, name);
			TableName[] tables = GetTables();
			TableName found = null;

			for (int i = 0; i < tables.Length; ++i) {
				bool match;
				if (case_insensitive) {
					match = tables[i].EqualsIgnoreCase(table_name);
				} else {
					match = tables[i].Equals(table_name);
				}
				if (match) {
					if (found != null) {
						throw new StatementException("Ambiguous reference: " + name);
					} else {
						found = tables[i];
					}
				}
			}

			if (found == null) {
				throw new StatementException("Object not found: " + name);
			}

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
			sequence_manager.FlushGenerator(name);
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
			if (IsReadOnly) {
				throw new Exception(
						  "Sequence operation not permitted for Read only transaction.");
			}
			// Check: if null sequence manager then sequence ops not allowed.
			if (sequence_manager == null) {
				throw new Exception("Sequence operations are not permitted.");
			}

			SequenceManager seq = sequence_manager;
			long val = seq.NextValue(this, name);
			// No synchronized because a DatabaseConnection should be single threaded
			// only.
			sequence_value_cache[name] = val;
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
			if (sequence_value_cache.ContainsKey(name)) {
				return (long)sequence_value_cache[name];
			} else {
				throw new StatementException(
				  "Current value for sequence generator " + name + " is not available.");
			}
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
			if (sequence_manager == null)
				throw new Exception("Sequence operations are not permitted.");

			SequenceManager seq = sequence_manager;
			seq.SetValue(this, name, value);

			sequence_value_cache[name] = value;
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
		/// <param name="table_name"></param>
		/// <remarks>
		/// Values returned by this method are guarenteed unique within this 
		/// table. This is true even across transactions.
		/// <para>
		/// <b>Note</b> This change can not be rolled back.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public long NextUniqueID(TableName table_name) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master == null)
				throw new StatementException("Table with name '" + table_name + "' could not be found to retrieve unique id.");

			return master.NextUniqueId;
		}

		/// <summary>
		/// Sets the unique id for the given table name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="unique_id"></param>
		/// <remarks>
		/// This must only be called under very controlled situations, such as 
		/// when altering a table or when we need to fix sequence corruption.
		/// </remarks>
		public void SetUniqueID(TableName table_name, long unique_id) {
			if (IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master == null)
				throw new StatementException("Table with name '" + table_name + "' could not be found to set unique id.");

			master.SetUniqueID(unique_id);
		}
	}
}