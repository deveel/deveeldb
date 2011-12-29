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
		private readonly Dictionary<TableName, SequenceGenerator> sequenceKeyMap;

		/// <summary>
		/// A static TObject that represents numeric 1.
		/// </summary>
		private static readonly TObject OneVal = TObject.CreateInt4(1);

		/// <summary>
		/// A static TObject that represents boolean true.
		/// </summary>
		private static readonly TObject TrueVal = TObject.CreateBoolean(true);

		internal SequenceManager(TableDataConglomerate conglomerate) {
			this.conglomerate = conglomerate;
			sequenceKeyMap = new Dictionary<TableName, SequenceGenerator>();
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
			SequenceGenerator generator;

			if (!sequenceKeyMap.TryGetValue(name, out generator)) {
				// This sequence generator is not in the cache so we need to query the
				// sequence table for this.
				Transaction sequenceAccessTransaction = GetTransaction();
				try {
					ITableDataSource seqi = sequenceAccessTransaction.GetTable(TableDataConglomerate.SysSequenceInfo);
					SimpleTableQuery query = new SimpleTableQuery(seqi);

					StringObject schemaVal = StringObject.FromString(name.Schema);
					StringObject nameVal = StringObject.FromString(name.Name);
					IList<int> list = query.SelectEqual(2, nameVal, 1, schemaVal);

					if (list.Count == 0) {
						throw new StatementException("Sequence generator '" + name + "' not found.");
					} else if (list.Count > 1) {
						throw new Exception("Assert failed: multiple sequence keys with same name.");
					}

					int rowIndex = list[0];
					TObject sid = seqi.GetCellContents(0, rowIndex);
					TObject sschema = seqi.GetCellContents(1, rowIndex);
					TObject sname = seqi.GetCellContents(2, rowIndex);
					TObject stype = seqi.GetCellContents(3, rowIndex);

					long idVal = sid.ToBigNumber().ToInt64();

					query.Dispose();

					// Is this a custom sequence generator?
					// (stype == 1) == true
					if (stype.IsEqual(OneVal).ValuesEqual(TrueVal)) {
						// Native generator.
						generator = new SequenceGenerator(idVal, name);
					} else {
						// Query the sequence table.
						ITableDataSource seq = sequenceAccessTransaction.GetTable(TableDataConglomerate.SysSequence);
						query = new SimpleTableQuery(seq);

						list = query.SelectEqual(0, sid);

						if (list.Count == 0)
							throw new Exception("Sequence table does not contain sequence information.");
						if (list.Count > 1)
							throw new Exception("Sequence table contains multiple generators for id.");

						rowIndex = list[0];
						BigNumber lastValue = seq.GetCellContents(1, rowIndex).ToBigNumber();
						BigNumber increment = seq.GetCellContents(2, rowIndex).ToBigNumber();
						BigNumber minvalue = seq.GetCellContents(3, rowIndex).ToBigNumber();
						BigNumber maxvalue = seq.GetCellContents(4, rowIndex).ToBigNumber();
						BigNumber start = seq.GetCellContents(5, rowIndex).ToBigNumber();
						BigNumber cache = seq.GetCellContents(6, rowIndex).ToBigNumber();
						bool cycle = seq.GetCellContents(7, rowIndex).ToBoolean();

						query.Dispose();

						generator = new SequenceGenerator(idVal, name,
							   lastValue.ToInt64(), increment.ToInt64(),
							   minvalue.ToInt64(), maxvalue.ToInt64(), start.ToInt64(),
							   cache.ToInt64(), cycle);

						// Put the generator in the cache
						sequenceKeyMap[name] = generator;
					}
				} finally {
					// Make sure we always close and commit the transaction.
					try {
						sequenceAccessTransaction.Commit();
					} catch (TransactionException e) {
						conglomerate.Logger.Error(this, e);
						throw new Exception("Transaction Error: " + e.Message, e);
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
			Transaction sequenceAccessTransaction = GetTransaction();
			try {
				// The sequence table
				IMutableTableDataSource seq = sequenceAccessTransaction.GetMutableTable(TableDataConglomerate.SysSequence);
				// Find the row with the id for this generator.
				SimpleTableQuery query = new SimpleTableQuery(seq);
				IList<int> list = query.SelectEqual(0, (BigNumber)generator.Id);
				// Checks
				if (list.Count == 0) {
					throw new StatementException("Sequence '" + generator.Name + "' not found.");
				} else if (list.Count > 1) {
					throw new Exception("Assert failed: multiple id for sequence.");
				}

				// Get the row position
				int rowIndex = list[0];

				// Create the DataRow
				DataRow dataRow = new DataRow(seq);

				// Set the content of the row data
				dataRow.SetValue(0, TObject.CreateInt8(generator.Id));
				dataRow.SetValue(1, TObject.CreateInt8(generator.LastValue));
				dataRow.SetValue(2, TObject.CreateInt8(generator.IncrementBy));
				dataRow.SetValue(3, TObject.CreateInt8(generator.MinValue));
				dataRow.SetValue(4, TObject.CreateInt8(generator.MaxValue));
				dataRow.SetValue(5, TObject.CreateInt8(generator.Start));
				dataRow.SetValue(6, TObject.CreateInt8(generator.Cache));
				dataRow.SetValue(7, TObject.CreateBoolean(generator.Cycle));

				// Update the row
				seq.UpdateRow(rowIndex, dataRow);

				// Dispose the resources
				query.Dispose();

			} finally {
				// Close and commit the transaction
				try {
					sequenceAccessTransaction.Commit();
				} catch (TransactionException e) {
					conglomerate.Logger.Error(this, e);
					throw new Exception("Transaction Error: " + e.Message, e);
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
				sequenceKeyMap.Remove(name);
			}
		}

		/// <summary>
		/// Adds an entry to the Sequence table for a native table in the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This acts as a gateway between the native sequence table function and 
		/// the custom sequence generator. 
		/// Note that some of the system tables and all of the VIEW tables will not 
		/// have native sequence generators and thus not have an entry in the 
		/// sequence table.
		/// </remarks>
		internal static void AddNativeTableGenerator(Transaction transaction, TableName tableName) {
			// If the SysSequence or SysSequenceInfo tables don't exist then 
			// We can't add or remove native tables
			if (tableName.Equals(TableDataConglomerate.SysSequence) ||
				tableName.Equals(TableDataConglomerate.SysSequenceInfo) ||
				!transaction.TableExists(TableDataConglomerate.SysSequence) ||
				!transaction.TableExists(TableDataConglomerate.SysSequenceInfo)) {
				return;
			}

			IMutableTableDataSource table = transaction.GetMutableTable(TableDataConglomerate.SysSequenceInfo);
			long uniqueId = transaction.NextUniqueID(TableDataConglomerate.SysSequenceInfo);

			DataRow dataRow = new DataRow(table);
			dataRow.SetValue(0, uniqueId);
			dataRow.SetValue(1, tableName.Schema);
			dataRow.SetValue(2, tableName.Name);
			dataRow.SetValue(3, 1);
			table.AddRow(dataRow);

		}

		/// <summary>
		/// Removes an entry in the Sequence table for a native table in the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		internal static void RemoveNativeTableGenerator(Transaction transaction, TableName tableName) {
			// If the SysSequence or SysSequenceInfo tables don't exist then 
			// We can't add or remove native tables
			if (tableName.Equals(TableDataConglomerate.SysSequence) ||
				tableName.Equals(TableDataConglomerate.SysSequenceInfo) ||
				!transaction.TableExists(TableDataConglomerate.SysSequence) ||
				!transaction.TableExists(TableDataConglomerate.SysSequenceInfo)) {
				return;
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq = transaction.GetMutableTable(TableDataConglomerate.SysSequence);
			IMutableTableDataSource seqi = transaction.GetMutableTable(TableDataConglomerate.SysSequenceInfo);

			SimpleTableQuery query = new SimpleTableQuery(seqi);
			IList<int> list = query.SelectEqual(2, TObject.CreateString(tableName.Name),
			                                    1, TObject.CreateString(tableName.Schema));

			// Remove the corresponding entry in the SEQUENCE table
			foreach (int rowIndex in list) {
				TObject sid = seqi.GetCellContents(0, rowIndex);

				SimpleTableQuery query2 = new SimpleTableQuery(seq);
				IList<int> list2 = query2.SelectEqual(0, sid);
				foreach (int rowIndex2 in list2) {
					// Remove entry from the sequence table.
					seq.RemoveRow(rowIndex2);
				}

				// Remove entry from the sequence info table
				seqi.RemoveRow(rowIndex);

				query2.Dispose();
			}

			query.Dispose();

		}

		internal static bool SequenceGeneratorExists(Transaction transaction, TableName tableName) {
			// If the SysSequence or SysSequenceInfo tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SysSequence) ||
			    !transaction.TableExists(TableDataConglomerate.SysSequenceInfo)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq = transaction.GetMutableTable(TableDataConglomerate.SysSequence);
			IMutableTableDataSource seqi = transaction.GetMutableTable(TableDataConglomerate.SysSequenceInfo);

			// All rows in 'sequence_info' that match this table name.
			using (SimpleTableQuery query = new SimpleTableQuery(seqi)) {
				IList<int> list = query.SelectEqual(2, TObject.CreateString(tableName.Name),
				                                    1, TObject.CreateString(tableName.Schema));
				return list.Count > 0;
			}
		}

		/// <summary>
		/// Creates a new sequence generator with the given name and details.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <param name="startValue"></param>
		/// <param name="incrementBy"></param>
		/// <param name="minValue"></param>
		/// <param name="maxValue"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// Note that this method does not check if the generator name clashes 
		/// with an existing database object.
		/// </remarks>
		internal static void CreateSequenceGenerator(Transaction transaction, TableName tableName, long startValue, long incrementBy, long minValue, long maxValue, long cache, bool cycle) {

			// If the SysSequence or SysSequenceInfo tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SysSequence) ||
				!transaction.TableExists(TableDataConglomerate.SysSequenceInfo)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// The SEQUENCE and SEQUENCE_INFO table
			IMutableTableDataSource seq = transaction.GetMutableTable(TableDataConglomerate.SysSequence);
			IMutableTableDataSource seqi = transaction.GetMutableTable(TableDataConglomerate.SysSequenceInfo);

			// All rows in 'sequence_info' that match this table name.
			using (SimpleTableQuery query = new SimpleTableQuery(seqi)) {
				IList<int> ivec =
					query.SelectEqual(2, TObject.CreateString(tableName.Name),
					                  1, TObject.CreateString(tableName.Schema));

				if (ivec.Count > 0)
					throw new Exception("Sequence generator with name '" + tableName + "' already exists.");

				// Dispose the query object
				// query.Dispose();
			}

			// Generate a unique id for the sequence info table
			long uniqueId = transaction.NextUniqueID(TableDataConglomerate.SysSequenceInfo);

			// Insert the new row
			DataRow dataRow = new DataRow(seqi);
			dataRow.SetValue(0, uniqueId);
			dataRow.SetValue(1, tableName.Schema);
			dataRow.SetValue(2, tableName.Name);
			dataRow.SetValue(3, 2);
			seqi.AddRow(dataRow);

			// Insert into the SEQUENCE table.
			dataRow = new DataRow(seq);
			dataRow.SetValue(0, uniqueId);
			dataRow.SetValue(1, startValue);
			dataRow.SetValue(2, incrementBy);
			dataRow.SetValue(3, minValue);
			dataRow.SetValue(4, maxValue);
			dataRow.SetValue(5, startValue);
			dataRow.SetValue(6, cache);
			dataRow.SetValue(7, cycle);
			seq.AddRow(dataRow);

		}

		 internal static void DropSequenceGenerator(Transaction transaction, TableName tableName) {
			// If the SysSequence or SysSequenceInfo tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.SysSequence) ||
				!transaction.TableExists(TableDataConglomerate.SysSequenceInfo)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// Remove the table generator (delete SEQUENCE_INFO and SEQUENCE entry)
			RemoveNativeTableGenerator(transaction, tableName);
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

				if (generator.Type == 1)
					// Native generator
					return transaction.NextUniqueID(new TableName(name.Schema, name.Name));

				// Custom sequence generator
				long currentVal = generator.CurrentValue;

				// Increment the current value.
				generator.IncrementCurrentValue();

				// Have we reached the current cached point?
				if (currentVal == generator.LastValue) {
					// Increment the generator
					for (int i = 0; i < generator.Cache; ++i) {
						generator.IncrementLastValue();
					}

					// Update the state
					UpdateGeneratorState(generator);

				}

				return generator.CurrentValue;
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

				if (generator.Type == 1)
					// Native generator
					return transaction.NextUniqueID(new TableName(name.Schema, name.Name));

				// Custom sequence generator
				return generator.CurrentValue;
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

				if (generator.Type == 1) {
					// Native generator
					transaction.SetUniqueID(new TableName(name.Schema, name.Name), value);
				} else {
					// Custom sequence generator
					generator.CurrentValue = value;
					generator.LastValue = value;

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
			public long CurrentValue;

			/// <summary>
			/// The id value of this sequence key.
			/// </summary>
			public readonly long Id;

			/// <summary>
			/// The name of this sequence key.
			/// </summary>
			public readonly TableName Name;

			/// <summary>
			/// The type of this sequence key.
			/// </summary>
			public readonly int Type;

			// The following values are only set if 'type' is not a native table
			// sequence.

			/// <summary>
			/// The last value of this sequence key.
			/// </summary>
			/// <remarks>
			/// This value represents the value of the sequence key in 
			/// the persistence medium.
			/// </remarks>
			public long LastValue;

			/// <summary>
			/// The number we increment the sequence key by.
			/// </summary>
			public readonly long IncrementBy;

			/// <summary>
			/// The minimum value of the sequence key.
			/// </summary>
			public readonly long MinValue;

			/// <summary>
			/// The maximum value of the sequence key.
			/// </summary>
			public readonly long MaxValue;

			/// <summary>
			/// The start value of the sequence generator.
			/// </summary>
			public readonly long Start;

			/// <summary>
			/// How many values we cache.
			/// </summary>
			public readonly long Cache;

			/// <summary>
			/// True if the sequence key is cycled.
			/// </summary>
			public readonly bool Cycle;

			// native generators
			public SequenceGenerator(long id, TableName name) {
				Type = 1;
				Id = id;
				Name = name;
			}

			internal SequenceGenerator(long id, TableName name, long lastValue,
							  long incrementBy, long minValue, long maxValue,
							  long start, long cache, bool cycle) {
				Type = 2;
				Id = id;
				Name = name;
				LastValue = lastValue;
				CurrentValue = lastValue;
				IncrementBy = incrementBy;
				MinValue = minValue;
				MaxValue = maxValue;
				Start = start;
				Cache = cache;
				Cycle = cycle;
			}

			private long IncrementValue(long val) {
				val += IncrementBy;
				if (val > MaxValue) {
					if (Cycle) {
						val = MinValue;
					} else {
						throw new StatementException("Sequence out of bounds.");
					}
				}
				if (val < MinValue) {
					if (Cycle) {
						val = MaxValue;
					} else {
						throw new StatementException("Sequence out of bounds.");
					}
				}
				return val;
			}

			public void IncrementCurrentValue() {
				CurrentValue = IncrementValue(CurrentValue);
			}

			public void IncrementLastValue() {
				LastValue = IncrementValue(LastValue);
			}
		}

		/// <summary>
		/// An object that models the list of sequences as table objects 
		/// in a transaction.
		/// </summary>
		private sealed class SequenceInternalTableInfo : IInternalTableInfo {
			private readonly Transaction transaction;

			internal SequenceInternalTableInfo(Transaction transaction) {
				this.transaction = transaction;
			}

			private static DataTableInfo CreateTableInfo(string schema, string name) {
				// Create the DataTableInfo that describes this entry
				DataTableInfo info = new DataTableInfo(new TableName(schema, name));

				// Add column definitions
				info.AddColumn("last_value", TType.NumericType);
				info.AddColumn("current_value", TType.NumericType);
				info.AddColumn("top_value", TType.NumericType);
				info.AddColumn("increment_by", TType.NumericType);
				info.AddColumn("min_value", TType.NumericType);
				info.AddColumn("max_value", TType.NumericType);
				info.AddColumn("start", TType.NumericType);
				info.AddColumn("cache", TType.NumericType);
				info.AddColumn("cycle", TType.BooleanType);

				// Set to immutable
				info.IsReadOnly = true;

				// Return the data table info
				return info;
			}

			public int TableCount {
				get {
					TableName SEQ = TableDataConglomerate.SysSequence;
					if (transaction.TableExists(SEQ))
						return transaction.GetTable(SEQ).RowCount;
					return 0;
				}
			}

			public int FindTableName(TableName name) {
				TableName seqInfo = TableDataConglomerate.SysSequenceInfo;
				if (transaction.RealTableExists(seqInfo)) {
					// Search the table.
					ITableDataSource table = transaction.GetTable(seqInfo);
					IRowEnumerator row_e = table.GetRowEnumerator();
					int p = 0;
					while (row_e.MoveNext()) {
						int row_index = row_e.RowIndex;
						TObject seqType = table.GetCellContents(3, row_index);
						if (!seqType.IsEqual(OneVal).ValuesEqual(TrueVal)) {
							TObject obName = table.GetCellContents(2, row_index);
							if (obName.Object.ToString().Equals(name.Name)) {
								TObject obSchema = table.GetCellContents(1, row_index);
								if (obSchema.Object.ToString().Equals(name.Schema)) {
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
				TableName seqInfo = TableDataConglomerate.SysSequenceInfo;
				if (transaction.RealTableExists(seqInfo)) {
					// Search the table.
					ITableDataSource table = transaction.GetTable(seqInfo);
					IRowEnumerator row_e = table.GetRowEnumerator();
					int p = 0;
					while (row_e.MoveNext()) {
						int rowIndex = row_e.RowIndex;
						TObject seqType = table.GetCellContents(3, rowIndex);
						if (!seqType.IsEqual(OneVal).ValuesEqual(TrueVal)) {
							if (i == p) {
								TObject obSchema = table.GetCellContents(1, rowIndex);
								TObject obName = table.GetCellContents(2, rowIndex);
								return new TableName(obSchema.Object.ToString(), obName.Object.ToString());
							}
							++p;
						}
					}
				}
				throw new Exception("Out of bounds.");
			}

			public bool ContainsTableName(TableName name) {
				TableName seqInfo = TableDataConglomerate.SysSequenceInfo;
				// This set can not contain the table that is backing it, so we always
				// return false for that.  This check stops an annoying recursive
				// situation for table name resolution.
				if (name.Equals(seqInfo))
					return false;

				return FindTableName(name) != -1;
			}

			public String GetTableType(int i) {
				return "SEQUENCE";
			}

			public DataTableInfo GetTableInfo(int i) {
				TableName tableName = GetTableName(i);
				return CreateTableInfo(tableName.Schema, tableName.Name);
			}

			public ITableDataSource CreateInternalTable(int index) {
				ITableDataSource table = transaction.GetTable(TableDataConglomerate.SysSequenceInfo);
				IRowEnumerator rowEnum = table.GetRowEnumerator();
				int p = 0;
				int i;
				int rowIndex = -1;
				while (rowEnum.MoveNext() && rowIndex == -1) {
					i = rowEnum.RowIndex;

					// Is this is a type 1 sequence we ignore (native table sequence).
					TObject seqType = table.GetCellContents(3, i);
					if (!seqType.IsEqual(OneVal).ValuesEqual(TrueVal)) {
						if (p == index) {
							rowIndex = i;
						}
						++p;
					}

				}
				if (rowIndex == -1)
					throw new Exception("Index out of bounds.");

				TObject seqId = table.GetCellContents(0, rowIndex);
				String schema = table.GetCellContents(1, rowIndex).Object.ToString();
				String name = table.GetCellContents(2, rowIndex).Object.ToString();

				TableName tableName = new TableName(schema, name);

				// Find this id in the 'sequence' table
				ITableDataSource seqTable = transaction.GetTable(TableDataConglomerate.SysSequence);
				SelectableScheme scheme = seqTable.GetColumnScheme(0);
				IList<int> list = scheme.SelectEqual(seqId);
				if (list.Count <= 0)
					throw new Exception("No SEQUENCE table entry for generator.");

				int seqRowI = list[0];

				// Generate the DataTableInfo
				DataTableInfo tableInfo = CreateTableInfo(schema, name);

				// Last value for this sequence generated by the transaction
				TObject lastValue;
				try {
					lastValue = TObject.CreateInt8(transaction.LastSequenceValue(tableName));
				} catch (StatementException) {
					lastValue = TObject.CreateInt8(-1);
				}

				// The current value of the sequence generator
				SequenceManager manager = transaction.Conglomerate.SequenceManager;
				TObject currentValue = TObject.CreateInt8(manager.CurrentValue(transaction, tableName));

				// Read the rest of the values from the SEQUENCE table.
				TObject topValue = seqTable.GetCellContents(1, seqRowI);
				TObject incrementBy = seqTable.GetCellContents(2, seqRowI);
				TObject minValue = seqTable.GetCellContents(3, seqRowI);
				TObject maxValue = seqTable.GetCellContents(4, seqRowI);
				TObject start = seqTable.GetCellContents(5, seqRowI);
				TObject cache = seqTable.GetCellContents(6, seqRowI);
				TObject cycle = seqTable.GetCellContents(7, seqRowI);

				// Implementation of IMutableTableDataSource that describes this
				// sequence generator.
				GTDataSourceImpl dataSource = new GTDataSourceImpl(transaction.System, tableInfo);
				dataSource.top_value = topValue;
				dataSource.last_value = lastValue;
				dataSource.current_value = currentValue;
				dataSource.increment_by = incrementBy;
				dataSource.min_value = minValue;
				dataSource.max_value = maxValue;
				dataSource.start = start;
				dataSource.cache = cache;
				dataSource.cycle = cycle;
				return dataSource;
			}

			private class GTDataSourceImpl : GTDataSource {
				private readonly DataTableInfo tableInfo;
				internal TObject last_value;
				internal TObject current_value;
				internal TObject top_value;
				internal TObject increment_by;
				internal TObject min_value;
				internal TObject max_value;
				internal TObject start;
				internal TObject cache;
				internal TObject cycle;

				public GTDataSourceImpl(TransactionSystem system, DataTableInfo tableInfo)
					: base(system) {
					this.tableInfo = tableInfo;
				}

				public override DataTableInfo TableInfo {
					get { return tableInfo; }
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