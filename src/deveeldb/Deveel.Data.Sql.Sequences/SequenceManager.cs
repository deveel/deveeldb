// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Linq;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Sequences {
	/// <summary>
	/// A default implementation of a sequence manager that is backed by 
	/// a given transaction.
	/// </summary>
	public sealed class SequenceManager : IObjectManager {
		/// <summary>
		/// A static TObject that represents numeric 1.
		/// </summary>
		private static readonly Field OneValue = Field.Integer(1);

		private ObjectCache<ISequence> seqCache;

		/// <summary>
		/// Construct a new instance of <see cref="SequenceManager"/> that is backed by
		/// the given transaction factory.
		/// </summary>
		/// <param name="transaction"></param>
		public SequenceManager(ITransaction transaction) {
			Transaction = transaction;
			seqCache = new ObjectCache<ISequence>();
		}

		~SequenceManager() {
			Dispose(false);
		}

		public static readonly ObjectName SequenceTableName = new ObjectName(SystemSchema.SchemaName, "sequence");

		public static readonly ObjectName SequenceInfoTableName = new ObjectName(SystemSchema.SchemaName, "sequence_info");

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Sequence; }
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (seqCache != null)
					seqCache.Dispose();
			}

			seqCache = null;
			Transaction = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Gets the transaction where the manager is operating.
		/// </summary>
		private ITransaction Transaction { get; set; }

		/// <summary>
		/// Updates the state of the sequence key in the sequence tables in the
		/// database.
		/// </summary>
		/// <param name="sequence"></param>
		/// <remarks>
		/// The update occurs on an independent transaction.
		/// </remarks>
		private void UpdateSequenceState(Sequence sequence) {
			// We need to update the sequence key state.

			// The sequence table
			var seq = Transaction.GetMutableTable(SequenceTableName);

			// Find the row with the id for this sequence.
			var list = seq.SelectRowsEqual(0, Field.Number(sequence.Id)).ToList();

			// Checks
			var count = list.Count();
			if (count == 0)
				throw new ObjectNotFoundException(sequence.FullName);

			if (count > 1)
				throw new Exception("Assert failed: multiple id for sequence.");

			// Create the DataRow
			var dataRow = seq.GetRow(list.First());

			// Set the content of the row data
			dataRow.SetValue(0, Field.Number(sequence.Id));
			dataRow.SetValue(1, Field.Number(sequence.LastValue));
			dataRow.SetValue(2, Field.Number(sequence.SequenceInfo.Increment));
			dataRow.SetValue(3, Field.Number(sequence.SequenceInfo.MinValue));
			dataRow.SetValue(4, Field.Number(sequence.SequenceInfo.MaxValue));
			dataRow.SetValue(5, Field.Number(sequence.SequenceInfo.StartValue));
			dataRow.SetValue(6, Field.BigInt(sequence.SequenceInfo.Cache));
			dataRow.SetValue(7, Field.Boolean(sequence.SequenceInfo.Cycle));

			// Update the row
			seq.UpdateRow(dataRow);
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var seqInfo = objInfo as SequenceInfo;
			if (seqInfo == null)
				throw new ArgumentException();

			CreateSequence(seqInfo);
		}

		public ISequence CreateSequence(SequenceInfo sequenceInfo) {
			if (sequenceInfo == null)
				throw new ArgumentNullException("sequenceInfo");

			var sequenceName = sequenceInfo.SequenceName;
			
			// If the Sequence or SequenceInfo tables don't exist then 
			// We can't add or remove native tables
			if (sequenceName.Equals(SequenceTableName) ||
				sequenceName.Equals(SequenceInfoTableName) ||
				!Transaction.TableExists(SequenceTableName) ||
				!Transaction.TableExists(SequenceInfoTableName)) {
				return null;
			}

			try {
				if (sequenceInfo.Type == SequenceType.Native)
					return CreateNativeTableSequence(sequenceName);

				return CreateCustomSequence(sequenceName, sequenceInfo);
			} finally {
				seqCache.Clear();
			}
		}

		private Sequence CreateCustomSequence(ObjectName sequenceName, SequenceInfo sequenceInfo) {
			// The SEQUENCE and SEQUENCE_INFO table
			var seq = Transaction.GetMutableTable(SequenceTableName);
			var seqi = Transaction.GetMutableTable(SequenceInfoTableName);

			var list = seqi.SelectRowsEqual(2, Field.VarChar(sequenceName.Name), 1, Field.VarChar(sequenceName.Parent.FullName));
			if (list.Any())
				throw new Exception(String.Format("Sequence generator with name '{0}' already exists.", sequenceName));

			// Generate a unique id for the sequence info table
			var uniqueId = Transaction.NextTableId(SequenceInfoTableName);

			// Insert the new row
			var dataRow = seqi.NewRow();
			dataRow.SetValue(0, Field.Number(uniqueId));
			dataRow.SetValue(1, Field.VarChar(sequenceName.Parent.FullName));
			dataRow.SetValue(2, Field.VarChar(sequenceName.Name));
			dataRow.SetValue(3, Field.BigInt(2));
			seqi.AddRow(dataRow);

			// Insert into the SEQUENCE table.
			dataRow = seq.NewRow();
			dataRow.SetValue(0, Field.Number(uniqueId));
			dataRow.SetValue(1, Field.Number(sequenceInfo.StartValue));
			dataRow.SetValue(2, Field.Number(sequenceInfo.Increment));
			dataRow.SetValue(3, Field.Number(sequenceInfo.MinValue));
			dataRow.SetValue(4, Field.Number(sequenceInfo.MaxValue));
			dataRow.SetValue(5, Field.Number(sequenceInfo.StartValue));
			dataRow.SetValue(6, Field.BigInt(sequenceInfo.Cache));
			dataRow.SetValue(7, Field.Boolean(sequenceInfo.Cycle));
			seq.AddRow(dataRow);

			return new Sequence(this, uniqueId, sequenceInfo);
		}

		private ISequence CreateNativeTableSequence(ObjectName tableName) {
			var table = Transaction.GetMutableTable(SequenceInfoTableName);
			var uniqueId = Transaction.NextTableId(SequenceInfoTableName);

			var dataRow = table.NewRow();
			dataRow.SetValue(0, Field.Number(uniqueId));
			dataRow.SetValue(1, Field.VarChar(tableName.Parent.FullName));
			dataRow.SetValue(2, Field.VarChar(tableName.Name));
			dataRow.SetValue(3, Field.BigInt(1));
			table.AddRow(dataRow);

			return new Sequence(this, uniqueId, SequenceInfo.Native(tableName));
		}

		internal int Offset(ObjectName sequenceName) {
			return seqCache.Offset(sequenceName, FindByName);
		}

		private int FindByName(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			if (sequenceName.Parent == null)
				return -1;

			var seqInfo = SequenceInfoTableName;
			if (!Transaction.RealTableExists(seqInfo))
				return -1;

			// Search the table.
			var table = Transaction.GetTable(seqInfo);
			var name = Field.VarChar(sequenceName.Name);
			var schema = Field.VarChar(sequenceName.Parent.FullName);

			int p = 0;
			foreach (var row in table) {
				var seqType = row.GetValue(3);
				if (!seqType.IsEqualTo(OneValue)) {
					var obName = row.GetValue(2);
					if (obName.IsEqualTo(name)) {
						var obSchema = row.GetValue(1);
						if (obSchema.IsEqualTo(schema)) {
							// Match so return this
							return p;
						}
					}

					++p;
				}
			}

			return -1;
		}

		internal ObjectName NameAt(int offset) {
			return seqCache.NameAt(offset, GetTableName);
		}

		private ObjectName GetTableName(int offset) {
			var seqInfo = SequenceInfoTableName;
			if (Transaction.RealTableExists(seqInfo)) {
				var table = Transaction.GetTable(seqInfo);
				int p = 0;
				foreach (var row in table) {
					var seqType = row.GetValue(3);
					if (!seqType.IsEqualTo(OneValue)) {
						if (offset == p) {
							var obSchema = row.GetValue(1);
							var obName = row.GetValue(2);
							return new ObjectName(ObjectName.Parse(obSchema.Value.ToString()), obName.Value.ToString());
						}
						++p;
					}
				}
			}

			return null;
		}


		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotImplementedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropSequence(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			return seqCache.ResolveName(objName, ignoreCase);
		}

		public bool DropSequence(ObjectName sequenceName) {
			// If the Sequence or SequenceInfo tables don't exist then 
			// we can't create the sequence sequence
			if (!Transaction.ObjectExists(SequenceTableName) ||
				!Transaction.ObjectExists(SequenceInfoTableName)) {
				throw new Exception("System sequence tables do not exist.");
			}

			try {
				// Remove the table sequence (delete SEQUENCE_INFO and SEQUENCE entry)
				return RemoveNativeTableSequence(sequenceName);
			} finally {
				seqCache.Clear();
			}
		}

		private bool RemoveNativeTableSequence(ObjectName tableName) {
			// If the Sequence or SequenceInfo tables don't exist then 
			// We can't add or remove native tables
			if (tableName.Equals(SequenceTableName) ||
				tableName.Equals(SequenceInfoTableName) ||
				!Transaction.ObjectExists(SequenceTableName) ||
				!Transaction.ObjectExists(SequenceInfoTableName)) {
				return false;
			}

			// The SEQUENCE and SEQUENCE_INFO table
			var seq = Transaction.GetMutableTable(SequenceTableName);
			var seqi = Transaction.GetMutableTable(SequenceInfoTableName);

			var list = seqi.SelectRowsEqual(2, Field.VarChar(tableName.Name), 1, Field.VarChar(tableName.Parent.FullName));

			// Remove the corresponding entry in the SEQUENCE table
			foreach (var rowIndex in list) {
				var sid = seqi.GetValue(rowIndex, 0);
				var list2 = seq.SelectRowsEqual(0, sid);
				foreach (int rowIndex2 in list2) {
					// Remove entry from the sequence table.
					seq.RemoveRow(rowIndex2);
				}

				// Remove entry from the sequence info table
				seqi.RemoveRow(rowIndex);
			}

			return true;
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return SequenceExists(objName);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return SequenceExists(objName);
		}

		public bool SequenceExists(ObjectName sequenceName) {
			// If the Sequence or SequenceInfo tables don't exist then 
			// we can't create the sequence generator
			if (!Transaction.TableExists(SequenceTableName) ||
				!Transaction.TableExists(SequenceInfoTableName)) {
				throw new Exception("System sequence tables do not exist.");
			}

			// The SEQUENCE and SEQUENCE_INFO table
			var seq = Transaction.GetMutableTable(SequenceTableName);
			var seqi = Transaction.GetMutableTable(SequenceInfoTableName);

			return seqi.SelectRowsEqual(1, Field.VarChar(sequenceName.ParentName), 2, Field.VarChar(sequenceName.Name)).Any();
		}


		private SqlNumber NextValue(ObjectName name) {
			lock (this) {
				var sequence = (Sequence)GetSequence(name);

				if (sequence.SequenceInfo.Type == SequenceType.Native)
					// Native generator
					return Transaction.NextTableId(name);

				// Custom sequence generator
				var currentVal = sequence.CurrentValue;

				// Increment the current value.
				sequence.IncrementCurrentValue();

				// Have we reached the current cached point?
				if (currentVal == sequence.LastValue) {
					// Increment the generator
					for (int i = 0; i < sequence.SequenceInfo.Cache; ++i) {
						sequence.IncrementLastValue();
					}

					// Update the state
					UpdateSequenceState(sequence);

				}

				return sequence.CurrentValue;
			}
		}

		private SqlNumber SetValue(ObjectName name, SqlNumber value) {
			lock (this) {
				var sequence = (Sequence) GetSequence(name);

				if (sequence.SequenceInfo.Type == SequenceType.Native)
					return Transaction.SetTableId(name, value);

				// Custom sequence generator
				sequence.CurrentValue = value;
				sequence.LastValue = value;

				// Update the state
				UpdateSequenceState(sequence);

				return value;
			}
		}

		public SqlNumber GetCurrentValue(ObjectName name) {
			lock (this) {
				var sequence = (Sequence) GetSequence(name);

				if (sequence.SequenceInfo.Type == SequenceType.Native)
					return Transaction.CurrentTableId(name);

				// Custom sequence generator
				return sequence.CurrentValue;
			}
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetSequence(objName);
		}

		public ISequence GetSequence(ObjectName sequenceName) {
			// Is the generator already in the cache?
			ISequence sequence;

			if (!seqCache.TryGet(sequenceName, out sequence)) {
				// This sequence generator is not in the cache so we need to query the
				// sequence table for this.
				var seqi = Transaction.GetTable(SequenceInfoTableName);

				var schemaVal = Field.VarChar(sequenceName.Parent.FullName);
				var nameVal = Field.VarChar(sequenceName.Name);
				var list = seqi.SelectRowsEqual(2, nameVal, 1, schemaVal).ToList();

				if (list.Count == 0) {
					throw new ArgumentException(String.Format("Sequence '{0}' not found.", sequenceName));
				} else if (list.Count() > 1) {
					throw new Exception("Assert failed: multiple sequence keys with same name.");
				}

				int rowIndex = list.First();
				var sid = seqi.GetValue(rowIndex, 0);
				var sschema = seqi.GetValue(rowIndex, 1);
				var sname = seqi.GetValue(rowIndex, 2);
				var stype = seqi.GetValue(rowIndex, 3);

				// Is this a custom sequence generator?
				// (stype == 1) == true
				if (stype.IsEqualTo(OneValue)) {
					// Native generator.
					sequence = new Sequence(this, (SqlNumber) sid.Value, SequenceInfo.Native(sequenceName));
				} else {
					// Query the sequence table.
					var seq = Transaction.GetTable(SequenceTableName);

					list = seq.SelectRowsEqual(0, sid).ToList();

					if (!list.Any())
						throw new Exception("Sequence table does not contain sequence information.");
					if (list.Count() > 1)
						throw new Exception("Sequence table contains multiple generators for id.");

					rowIndex = list.First();
					var lastValue = (SqlNumber) seq.GetValue(rowIndex, 1).Value;
					var increment = (SqlNumber) seq.GetValue(rowIndex, 2).Value;
					var minvalue = (SqlNumber) seq.GetValue(rowIndex, 3).Value;
					var maxvalue = (SqlNumber) seq.GetValue(rowIndex, 4).Value;
					var start = (SqlNumber) seq.GetValue(rowIndex, 5).Value;
					var cache = (long) seq.GetValue(rowIndex, 6).AsBigInt();
					bool cycle = seq.GetValue(rowIndex, 7).AsBoolean();

					var info = new SequenceInfo(sequenceName, start, increment, minvalue, maxvalue, cache, cycle);
					sequence = new Sequence(this, (SqlNumber) sid.Value, lastValue, info);

					// Put the generator in the cache
					seqCache.Set(sequenceName, sequence);
				}

			}

			// Return the generator
			return sequence;

		}

		#region Sequence

		class Sequence : ISequence {
			private readonly SequenceManager manager;

			public Sequence(SequenceManager manager, SqlNumber id, SequenceInfo sequenceInfo) 
				: this(manager, id, SqlNumber.Null, sequenceInfo) {
			}

			public Sequence(SequenceManager manager, SqlNumber id, SqlNumber lastValue, SequenceInfo sequenceInfo) {
				this.manager = manager;
				Id = id;
				SequenceInfo = sequenceInfo;
				LastValue = lastValue;
				CurrentValue = lastValue;
			}

			public SqlNumber Id { get; private set; }

			public SequenceInfo SequenceInfo { get; private set; }

			public ObjectName FullName {
				get { return SequenceInfo.SequenceName; }
			}

			IObjectInfo IDbObject.ObjectInfo {
				get { return SequenceInfo; }
			}

			public SqlNumber LastValue { get; set; }

			public SqlNumber CurrentValue { get; set; }

			public SqlNumber GetCurrentValue() {
				return manager.GetCurrentValue(FullName);
			}

			public SqlNumber NextValue() {
				return manager.NextValue(FullName);
			}

			public SqlNumber SetValue(SqlNumber value) {
				return manager.SetValue(FullName, value);
			}

			private SqlNumber IncrementValue(SqlNumber val) {
				val += SequenceInfo.Increment;
				if (val > SequenceInfo.MaxValue) {
					if (SequenceInfo.Cycle) {
						val = SequenceInfo.MinValue;
					} else {
						throw new InvalidOperationException("Sequence out of bounds.");
					}
				}
				if (val < SequenceInfo.MinValue) {
					if (SequenceInfo.Cycle) {
						val = SequenceInfo.MaxValue;
					} else {
						throw new InvalidOperationException("Sequence out of bounds.");
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

		#endregion
	}
}
