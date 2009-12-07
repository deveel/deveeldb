//  
//  SequenceManager.cs
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

using Deveel.Data.Collections;
using Deveel.Diagnostics;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Manages the creation and removal of sequence keys, and offers access 
	/// to the sequence values (possibly cached).
	/// </summary>
	/// <remarks>
	/// When the sequence table is changed, this opens an optimized transaction 
	/// on the database and manipulates the <i>SequenceInfo</i> table.
	/// </remarks>
	sealed class SequenceManager {
		/// <summary>
		/// The TableDataConglomerate object.
		/// </summary>
		private readonly TableDataConglomerate conglomerate;

		/// <summary>
		/// A hashmap that maps from the TableName of the sequence key
		/// to the object that manages this sequence (SequenceGenerator).
		/// (TableName) -> (SequenceGenerator)
		/// </summary>
		private readonly Hashtable sequence_key_map;

		/// <summary>
		/// A static TObject that represents numeric 1.
		/// </summary>
		private static readonly TObject OneVal = TObject.GetInt4(1);

		/// <summary>
		/// A static TObject that represents boolean true.
		/// </summary>
		private static readonly TObject TrueVal = TObject.GetBoolean(true);

		internal SequenceManager(TableDataConglomerate conglomerate) {
			this.conglomerate = conglomerate;
			sequence_key_map = new Hashtable();
		}

		/// <summary>
		/// Returns a new Transaction object for manipulating and querying the system state.
		/// </summary>
		private Transaction GetTransaction() {
			// Should this transaction be optimized for the access patterns we generate
			// here?
			return conglomerate.CreateTransaction();
		}

		/// <summary>
		/// Returns a <see cref="SequenceGenerator"/> object representing the 
		/// sequence generator with the given name.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		private SequenceGenerator GetGenerator(TableName name) {
			// Is the generator already in the cache?
			SequenceGenerator generator =
									 (SequenceGenerator)sequence_key_map[name];

			if (generator == null) {
				// This sequence generator is not in the cache so we need to query the
				// sequence table for this.
				Transaction sequence_access_transaction = GetTransaction();
				try {
					IMutableTableDataSource seqi =
						  sequence_access_transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
					SimpleTableQuery query = new SimpleTableQuery(seqi);

					StringObject schema_val = StringObject.FromString(name.Schema);
					StringObject name_val = StringObject.FromString(name.Name);
					IntegerVector ivec = query.SelectEqual(2, name_val, 1, schema_val);

					if (ivec.Count == 0) {
						throw new StatementException("Sequence generator '" + name +
													 "' not found.");
					} else if (ivec.Count > 1) {
						throw new Exception("Assert failed: multiple sequence keys with same name.");
					}

					int row_i = ivec[0];
					TObject sid = seqi.GetCellContents(0, row_i);
					TObject sschema = seqi.GetCellContents(1, row_i);
					TObject sname = seqi.GetCellContents(2, row_i);
					TObject stype = seqi.GetCellContents(3, row_i);

					long id_val = sid.ToBigNumber().ToInt64();

					query.Dispose();

					// Is this a custom sequence generator?
					// (stype == 1) == true
					if (stype.IsEqual(OneVal).ValuesEqual(TrueVal)) {
						// Native generator.
						generator = new SequenceGenerator(id_val, name);
					} else {
						// Query the sequence table.
						IMutableTableDataSource seq =
							sequence_access_transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE);
						query = new SimpleTableQuery(seq);

						ivec = query.SelectEqual(0, sid);

						if (ivec.Count == 0) {
							throw new Exception(
									  "Sequence table does not contain sequence information.");
						}
						if (ivec.Count > 1) {
							throw new Exception(
										"Sequence table contains multiple generators for id.");
						}

						row_i = ivec[0];
						BigNumber last_value = seq.GetCellContents(1, row_i).ToBigNumber();
						BigNumber increment = seq.GetCellContents(2, row_i).ToBigNumber();
						BigNumber minvalue = seq.GetCellContents(3, row_i).ToBigNumber();
						BigNumber maxvalue = seq.GetCellContents(4, row_i).ToBigNumber();
						BigNumber start = seq.GetCellContents(5, row_i).ToBigNumber();
						BigNumber cache = seq.GetCellContents(6, row_i).ToBigNumber();
						bool cycle = seq.GetCellContents(7, row_i).ToBoolean();

						query.Dispose();

						generator = new SequenceGenerator(id_val, name,
							   last_value.ToInt64(), increment.ToInt64(),
							   minvalue.ToInt64(), maxvalue.ToInt64(), start.ToInt64(),
							   cache.ToInt64(), cycle);

						// Put the generator in the cache
						sequence_key_map[name] = generator;

					}

				} finally {
					// Make sure we always close and commit the transaction.
					try {
						sequence_access_transaction.Commit();
					} catch (TransactionException e) {
						conglomerate.Debug.WriteException(e);
						throw new Exception("Transaction Error: " + e.Message);
					}
				}

			}

			// Return the generator
			return generator;
		}

		/// <summary>
		/// Updates the state of the sequence key in the sequence tables in the
		/// database.
		/// </summary>
		/// <param name="generator"></param>
		/// <remarks>
		/// The update occurs on an independant transaction.
		/// </remarks>
		private void UpdateGeneratorState(SequenceGenerator generator) {
			// We need to update the sequence key state.
			Transaction sequence_access_transaction = GetTransaction();
			try {
				// The sequence table
				IMutableTableDataSource seq = sequence_access_transaction.GetTable(
												  TableDataConglomerate.SYS_SEQUENCE);
				// Find the row with the id for this generator.
				SimpleTableQuery query = new SimpleTableQuery(seq);
				IntegerVector ivec = query.SelectEqual(0, (BigNumber)generator.id);
				// Checks
				if (ivec.Count == 0) {
					throw new StatementException("Sequence '" + generator.name + "' not found.");
				} else if (ivec.Count > 1) {
					throw new Exception("Assert failed: multiple id for sequence.");
				}

				// Get the row position
				int row_i = ivec[0];

				// Create the RowData
				RowData row_data = new RowData(seq);

				// Set the content of the row data
				row_data.SetColumnDataFromTObject(0, TObject.GetInt8(generator.id));
				row_data.SetColumnDataFromTObject(1, TObject.GetInt8(generator.last_value));
				row_data.SetColumnDataFromTObject(2, TObject.GetInt8(generator.increment_by));
				row_data.SetColumnDataFromTObject(3, TObject.GetInt8(generator.min_value));
				row_data.SetColumnDataFromTObject(4, TObject.GetInt8(generator.max_value));
				row_data.SetColumnDataFromTObject(5, TObject.GetInt8(generator.start));
				row_data.SetColumnDataFromTObject(6, TObject.GetInt8(generator.cache));
				row_data.SetColumnDataFromTObject(7, TObject.GetBoolean(generator.cycle));

				// Update the row
				seq.UpdateRow(row_i, row_data);

				// Dispose the resources
				query.Dispose();

			} finally {
				// Close and commit the transaction
				try {
					sequence_access_transaction.Commit();
				} catch (TransactionException e) {
					conglomerate.Debug.WriteException(e);
					throw new Exception("Transaction Error: " + e.Message);
				}
			}

		}

		/// <summary>
		/// Flushes a sequence generator from the cache.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This should be used when a sequence generator is altered or dropped 
		/// from the database.
		/// </remarks>
		internal void FlushGenerator(TableName name) {
			lock (this) {
				sequence_key_map.Remove(name);
			}
		}

		/// <summary>
		/// Adds an entry to the Sequence table for a native table in the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This acts as a gateway between the native sequence table function and 
		/// the custom sequence generator. 
		/// Note that some of the system tables and all of the VIEW tables will not 
		/// have native sequence generators and thus not have an entry in the 
		/// sequence table.
		/// </remarks>
		internal static void AddNativeTableGenerator(Transaction transaction, TableName table_name) {
			// If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then 
			// We can't add or remove native tables
			if (table_name.Equals(TableDataConglomerate.SYS_SEQUENCE) ||
				table_name.Equals(TableDataConglomerate.SYS_SEQUENCE_INFO) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
				return;
			}

			IMutableTableDataSource table =
						transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
			long unique_id =
					transaction.NextUniqueID(TableDataConglomerate.SYS_SEQUENCE_INFO);

			RowData row_data = new RowData(table);
			row_data.SetColumnDataFromObject(0, unique_id);
			row_data.SetColumnDataFromObject(1, table_name.Schema);
			row_data.SetColumnDataFromObject(2, table_name.Name);
			row_data.SetColumnDataFromObject(3, 1);
			table.AddRow(row_data);

		}

		/// <summary>
		/// Removes an entry in the Sequence table for a native table in the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		internal static void RemoveNativeTableGenerator(Transaction transaction, TableName table_name) {
			// If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then 
			// We can't add or remove native tables
			if (table_name.Equals(TableDataConglomerate.SYS_SEQUENCE) ||
				table_name.Equals(TableDataConglomerate.SYS_SEQUENCE_INFO) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
				return;
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq =
							 transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE);
			IMutableTableDataSource seqi =
						transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);

			SimpleTableQuery query = new SimpleTableQuery(seqi);
			IntegerVector ivec =
				query.SelectEqual(2, TObject.GetString(table_name.Name),
										 1, TObject.GetString(table_name.Schema));

			// Remove the corresponding entry in the SEQUENCE table
			for (int i = 0; i < ivec.Count; ++i) {
				int row_i = ivec[i];
				TObject sid = seqi.GetCellContents(0, row_i);

				SimpleTableQuery query2 = new SimpleTableQuery(seq);
				IntegerVector ivec2 = query2.SelectEqual(0, sid);
				for (int n = 0; n < ivec2.Count; ++n) {
					// Remove entry from the sequence table.
					seq.RemoveRow(ivec2[n]);
				}

				// Remove entry from the sequence info table
				seqi.RemoveRow(row_i);

				query2.Dispose();

			}

			query.Dispose();

		}

		internal static bool SequenceGeneratorExists(Transaction transaction, TableName table_name) {
			// If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq =
							 transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE);
			IMutableTableDataSource seqi =
						transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);

			// All rows in 'sequence_info' that match this table name.
			SimpleTableQuery query = new SimpleTableQuery(seqi);
			IntegerVector ivec =
				query.SelectEqual(2, TObject.GetString(table_name.Name),
										 1, TObject.GetString(table_name.Schema));

			query.Dispose();

			return ivec.Count > 0;
		}

		/// <summary>
		/// Creates a new sequence generator with the given name and details.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <param name="start_value"></param>
		/// <param name="increment_by"></param>
		/// <param name="min_value"></param>
		/// <param name="max_value"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// Note that this method does not check if the generator name clashes 
		/// with an existing database object.
		/// </remarks>
		internal static void CreateSequenceGenerator(Transaction transaction,
					 TableName table_name, long start_value, long increment_by,
					 long min_value, long max_value, long cache, bool cycle) {

			// If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq =
							 transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE);
			IMutableTableDataSource seqi =
						transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);

			// All rows in 'sequence_info' that match this table name.
			using (SimpleTableQuery query = new SimpleTableQuery(seqi)) {
				IntegerVector ivec =
					query.SelectEqual(2, TObject.GetString(table_name.Name),
					                  1, TObject.GetString(table_name.Schema));

				if (ivec.Count > 0)
					throw new Exception("Sequence generator with name '" + table_name + "' already exists.");

				// Dispose the query object
				// query.Dispose();
			}

			// Generate a unique id for the sequence info table
			long unique_id =
					transaction.NextUniqueID(TableDataConglomerate.SYS_SEQUENCE_INFO);

			// Insert the new row
			RowData row_data = new RowData(seqi);
			row_data.SetColumnDataFromObject(0, unique_id);
			row_data.SetColumnDataFromObject(1, table_name.Schema);
			row_data.SetColumnDataFromObject(2, table_name.Name);
			row_data.SetColumnDataFromObject(3, 2);
			seqi.AddRow(row_data);

			// Insert into the SEQUENCE table.
			row_data = new RowData(seq);
			row_data.SetColumnDataFromObject(0, unique_id);
			row_data.SetColumnDataFromObject(1, start_value);
			row_data.SetColumnDataFromObject(2, increment_by);
			row_data.SetColumnDataFromObject(3, min_value);
			row_data.SetColumnDataFromObject(4, max_value);
			row_data.SetColumnDataFromObject(5, start_value);
			row_data.SetColumnDataFromObject(6, cache);
			row_data.SetColumnDataFromObject(7, cycle);
			seq.AddRow(row_data);

		}

		 internal static void DropSequenceGenerator(Transaction transaction, TableName table_name) {
			// If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE) ||
				!transaction.TableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// Remove the table generator (delete SEQUENCE_INFO and SEQUENCE entry)
			RemoveNativeTableGenerator(transaction, table_name);
		}

		 /// <summary>
		 /// Returns the next value from the sequence generator.
		 /// </summary>
		 /// <param name="transaction"></param>
		 /// <param name="name"></param>
		 /// <remarks>
		 /// This will atomically increment the sequence counter.
		 /// </remarks>
		 /// <returns></returns>
		internal long NextValue(SimpleTransaction transaction, TableName name) {
			lock (this) {
				SequenceGenerator generator = GetGenerator(name);

				if (generator.type == 1) {
					// Native generator
					return transaction.NextUniqueID(
						new TableName(name.Schema, name.Name));
				} else {
					// Custom sequence generator
					long current_val = generator.current_val;

					// Increment the current value.
					generator.IncrementCurrentValue();

					// Have we reached the current cached point?
					if (current_val == generator.last_value) {
						// Increment the generator
						for (int i = 0; i < generator.cache; ++i) {
							generator.IncrementLastValue();
						}

						// Update the state
						UpdateGeneratorState(generator);

					}

					return generator.current_val;
				}
			}
		}

		/// <summary>
		/// Returns the current value from the sequence generator.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		internal long CurrentValue(SimpleTransaction transaction, TableName name) {
			lock (this) {
				SequenceGenerator generator = GetGenerator(name);

				if (generator.type == 1) {
					// Native generator
					return transaction.NextUniqueID(new TableName(name.Schema, name.Name));
				} else {
					// Custom sequence generator
					return generator.current_val;
				}

			}
		}

		/// <summary>
		/// Sets the current value of the sequence generator.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="name"></param>
		/// <param name="value"></param>
		internal void SetValue(SimpleTransaction transaction, TableName name, long value) {
			lock (this) {
				SequenceGenerator generator = GetGenerator(name);

				if (generator.type == 1) {
					// Native generator
					transaction.SetUniqueID(
						new TableName(name.Schema, name.Name), value);
				} else {
					// Custom sequence generator
					generator.current_val = value;
					generator.last_value = value;

					// Update the state
					UpdateGeneratorState(generator);

				}

			}
		}


		/// <summary>
		/// Returns an IInternalTableInfo object used to model the list of sequence
		/// generators that are accessible within the given Transaction object.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is used to model all sequence generators that have been defined as tables.
		/// </remarks>
		/// <returns></returns>
		internal static IInternalTableInfo CreateInternalTableInfo(Transaction transaction) {
			return new SequenceInternalTableInfo(transaction);
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// Encapsulates information about the sequence key.
		/// </summary>
		private sealed class SequenceGenerator {

			/// <summary>
			/// The current value of this sequence generator.
			/// </summary>
			internal long current_val;

			/// <summary>
			/// The id value of this sequence key.
			/// </summary>
			internal long id;

			/// <summary>
			/// The name of this sequence key.
			/// </summary>
			internal TableName name;

			/// <summary>
			/// The type of this sequence key.
			/// </summary>
			internal int type;

			// The following values are only set if 'type' is not a native table
			// sequence.

			/// <summary>
			/// The last value of this sequence key.
			/// </summary>
			/// <remarks>
			/// This value represents the value of the sequence key in 
			/// the persistence medium.
			/// </remarks>
			internal long last_value;

			/// <summary>
			/// The number we increment the sequence key by.
			/// </summary>
			internal long increment_by;

			/// <summary>
			/// The minimum value of the sequence key.
			/// </summary>
			internal long min_value;

			/// <summary>
			/// The maximum value of the sequence key.
			/// </summary>
			internal long max_value;

			/// <summary>
			/// The start value of the sequence generator.
			/// </summary>
			internal long start;

			/// <summary>
			/// How many values we cache.
			/// </summary>
			internal long cache;

			/// <summary>
			/// True if the sequence key is cycled.
			/// </summary>
			internal bool cycle;


			internal SequenceGenerator(long id, TableName name) {
				type = 1;
				this.id = id;
				this.name = name;
			}

			internal SequenceGenerator(long id, TableName name, long last_value,
							  long increment_by, long min_value, long max_value,
							  long start, long cache, bool cycle) {
				type = 2;
				this.id = id;
				this.name = name;
				this.last_value = last_value;
				this.current_val = last_value;
				this.increment_by = increment_by;
				this.min_value = min_value;
				this.max_value = max_value;
				this.start = start;
				this.cache = cache;
				this.cycle = cycle;
			}

			private long IncrementValue(long val) {
				val += increment_by;
				if (val > max_value) {
					if (cycle) {
						val = min_value;
					} else {
						throw new StatementException("Sequence out of bounds.");
					}
				}
				if (val < min_value) {
					if (cycle) {
						val = max_value;
					} else {
						throw new StatementException("Sequence out of bounds.");
					}
				}
				return val;
			}

			internal void IncrementCurrentValue() {
				current_val = IncrementValue(current_val);
			}

			internal void IncrementLastValue() {
				last_value = IncrementValue(last_value);
			}
		}

		/// <summary>
		/// An object that models the list of sequences as table objects 
		/// in a transaction.
		/// </summary>
		private sealed class SequenceInternalTableInfo : IInternalTableInfo {

			Transaction transaction;

			internal SequenceInternalTableInfo(Transaction transaction) {
				this.transaction = transaction;
			}

			private static DataTableDef createDataTableDef(String schema, String name) {
				// Create the DataTableDef that describes this entry
				DataTableDef def = new DataTableDef();
				def.TableName = new TableName(schema, name);

				// Add column definitions
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("last_value"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("current_value"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("top_value"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("increment_by"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("min_value"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("max_value"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("start"));
				def.AddColumn(DataTableColumnDef.CreateNumericColumn("cache"));
				def.AddColumn(DataTableColumnDef.CreateBooleanColumn("cycle"));

				// Set to immutable
				def.SetImmutable();

				// Return the data table def
				return def;
			}

			public int TableCount {
				get {
					TableName SEQ = TableDataConglomerate.SYS_SEQUENCE;
					if (transaction.TableExists(SEQ)) {
						return transaction.GetTable(SEQ).RowCount;
					} else {
						return 0;
					}
				}
			}

			public int FindTableName(TableName name) {
				TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
				if (transaction.RealTableExists(SEQ_INFO)) {
					// Search the table.
					IMutableTableDataSource table = transaction.GetTable(SEQ_INFO);
					IRowEnumerator row_e = table.GetRowEnumerator();
					int p = 0;
					while (row_e.MoveNext()) {
						int row_index = row_e.RowIndex;
						TObject seq_type = table.GetCellContents(3, row_index);
						if (!seq_type.IsEqual(OneVal).ValuesEqual(TrueVal)) {
							TObject ob_name = table.GetCellContents(2, row_index);
							if (ob_name.Object.ToString().Equals(name.Name)) {
								TObject ob_schema = table.GetCellContents(1, row_index);
								if (ob_schema.Object.ToString().Equals(name.Schema)) {
									// Match so return this
									return p;
								}
							}
							++p;
						}
					}
				}
				return -1;
			}

			public TableName GetTableName(int i) {
				TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
				if (transaction.RealTableExists(SEQ_INFO)) {
					// Search the table.
					IMutableTableDataSource table = transaction.GetTable(SEQ_INFO);
					IRowEnumerator row_e = table.GetRowEnumerator();
					int p = 0;
					while (row_e.MoveNext()) {
						int row_index = row_e.RowIndex;
						TObject seq_type = table.GetCellContents(3, row_index);
						if (!seq_type.IsEqual(OneVal).ValuesEqual(TrueVal)) {
							if (i == p) {
								TObject ob_schema = table.GetCellContents(1, row_index);
								TObject ob_name = table.GetCellContents(2, row_index);
								return new TableName(ob_schema.Object.ToString(),
													 ob_name.Object.ToString());
							}
							++p;
						}
					}
				}
				throw new Exception("Out of bounds.");
			}

			public bool ContainsTableName(TableName name) {
				TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
				// This set can not contain the table that is backing it, so we always
				// return false for that.  This check stops an annoying recursive
				// situation for table name resolution.
				if (name.Equals(SEQ_INFO)) {
					return false;
				} else {
					return FindTableName(name) != -1;
				}
			}

			public String GetTableType(int i) {
				return "SEQUENCE";
			}

			public DataTableDef GetDataTableDef(int i) {
				TableName table_name = GetTableName(i);
				return createDataTableDef(table_name.Schema, table_name.Name);
			}

			public IMutableTableDataSource CreateInternalTable(int index) {
				IMutableTableDataSource table =
						   transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
				IRowEnumerator row_e = table.GetRowEnumerator();
				int p = 0;
				int i;
				int row_i = -1;
				while (row_e.MoveNext() && row_i == -1) {
					i = row_e.RowIndex;

					// Is this is a type 1 sequence we ignore (native table sequence).
					TObject seq_type = table.GetCellContents(3, i);
					if (!seq_type.IsEqual(OneVal).ValuesEqual(TrueVal)) {
						if (p == index) {
							row_i = i;
						}
						++p;
					}

				}
				if (row_i != -1) {
					TObject seq_id = table.GetCellContents(0, row_i);
					String schema = table.GetCellContents(1, row_i).Object.ToString();
					String name = table.GetCellContents(2, row_i).Object.ToString();

					TableName table_name = new TableName(schema, name);

					// Find this id in the 'sequence' table
					IMutableTableDataSource seq_table =
								  transaction.GetTable(TableDataConglomerate.SYS_SEQUENCE);
					SelectableScheme scheme = seq_table.GetColumnScheme(0);
					IntegerVector ivec = scheme.SelectEqual(seq_id);
					if (ivec.Count > 0) {
						int seq_row_i = ivec[0];

						// Generate the DataTableDef
						DataTableDef table_def = createDataTableDef(schema, name);

						// Last value for this sequence generated by the transaction
						TObject lv;
						try {
							lv = TObject.GetInt8(transaction.LastSequenceValue(table_name));
						} catch (StatementException) {
							lv = TObject.GetInt8(-1);
						}
						TObject last_value = lv;
						// The current value of the sequence generator
						SequenceManager manager =
										  transaction.Conglomerate.SequenceManager;
						TObject current_value =
								  TObject.GetInt8(manager.CurrentValue(transaction, table_name));

						// Read the rest of the values from the SEQUENCE table.
						TObject top_value = seq_table.GetCellContents(1, seq_row_i);
						TObject increment_by = seq_table.GetCellContents(2, seq_row_i);
						TObject min_value = seq_table.GetCellContents(3, seq_row_i);
						TObject max_value = seq_table.GetCellContents(4, seq_row_i);
						TObject start = seq_table.GetCellContents(5, seq_row_i);
						TObject cache = seq_table.GetCellContents(6, seq_row_i);
						TObject cycle = seq_table.GetCellContents(7, seq_row_i);

						// Implementation of IMutableTableDataSource that describes this
						// sequence generator.
						GTDataSourceImpl data_source = new GTDataSourceImpl(transaction.System, table_def);
						data_source.top_value = top_value;
						data_source.last_value = last_value;
						data_source.current_value = current_value;
						data_source.increment_by = increment_by;
						data_source.min_value = min_value;
						data_source.max_value = max_value;
						data_source.start = start;
						data_source.cache = cache;
						data_source.cycle = cycle;
						return data_source;

					} else {
						throw new Exception("No SEQUENCE table entry for generator.");
					}

				} else {
					throw new Exception("Index out of bounds.");
				}

			}

			private class GTDataSourceImpl : GTDataSource {
				private readonly DataTableDef table_def;
				internal TObject last_value;
				internal TObject current_value;
				internal TObject top_value;
				internal TObject increment_by;
				internal TObject min_value;
				internal TObject max_value;
				internal TObject start;
				internal TObject cache;
				internal TObject cycle;

				public GTDataSourceImpl(TransactionSystem system, DataTableDef tableDef)
					: base(system) {
					table_def = tableDef;
				}

				public override DataTableDef DataTableDef {
					get { return table_def; }
				}

				public override int RowCount {
					get { return 1; }
				}

				public override TObject GetCellContents(int col, int row) {
					switch (col) {
						case 0:
							return last_value;
						case 1:
							return current_value;
						case 2:
							return top_value;
						case 3:
							return increment_by;
						case 4:
							return min_value;
						case 5:
							return max_value;
						case 6:
							return start;
						case 7:
							return cache;
						case 8:
							return cycle;
						default:
							throw new Exception("Column out of bounds.");
					}
				}
			}
		}
	}
}