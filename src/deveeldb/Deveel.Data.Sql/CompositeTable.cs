using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	class CompositeTable : Table, IRootTable {
		private readonly ITable mainTable;
		private readonly ITable[] composites;

		private readonly IList<int>[] rowIndexes;
		private readonly ColumnIndex[] columnIndexes;

		private int rootsLocked;

		public CompositeTable(ITable mainTable, ITable[] composites, CompositeFunction function, bool all) {
			this.mainTable = mainTable;
			this.composites = composites;

			columnIndexes = new ColumnIndex[mainTable.TableInfo.ColumnCount];
			int size = composites.Length;
			rowIndexes = new IList<int>[size];

			if (function == CompositeFunction.Union) {
				// Include all row sets in all tables
				for (int i = 0; i < size; ++i) {
					rowIndexes[i] = composites[i].SelectAllRows().ToList();
				}

				RemoveDuplicates(all);
			} else {
				throw new ApplicationException("Unrecognised composite function");
			}

		}

		private void RemoveDuplicates(bool all) {
			if (!all)
				throw new NotImplementedException();
		}

		public CompositeTable(ITable[] composites, CompositeFunction function, bool all)
			: this(composites[0], composites, function, all) {
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		public override IDatabaseContext DatabaseContext {
			get { return mainTable.DatabaseContext; }
		}

		protected override int ColumnCount {
			get { return mainTable.TableInfo.ColumnCount; }
		}

		public override void LockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked++;
			for (int i = 0; i < composites.Length; ++i) {
				composites[i].LockRoot(lockKey);
			}
		}

		public override void UnlockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked--;
			for (int i = 0; i < composites.Length; ++i) {
				composites[i].UnlockRoot(lockKey);
			}
		}

		public override int RowCount {
			get { return rowIndexes.Sum(t => t.Count); }
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			var index = columnIndexes[column];
			if (index == null) {
				index = new BlindSearchIndex(this, column);
				columnIndexes[column] = index;
			}

			// If we are getting a scheme for this table, simple return the information
			// from the column_trees Vector.
			if (table == this)
				return index;

			// Otherwise, get the scheme to calculate a subset of the given scheme.
			return index.GetSubset(table, originalColumn);
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (ancestor != this)
				throw new InvalidOperationException();

			return rowSet;
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			for (int i = 0; i < rowIndexes.Length; ++i) {
				var list = rowIndexes[i];
				int sz = list.Count;
				if (rowNumber < sz)
					return composites[i].GetValue(list[(int)rowNumber], columnOffset);

				rowNumber -= sz;
			}

			throw new ArgumentOutOfRangeException("rowNumber", rowNumber, String.Format("Row '{0}' out of range.", rowNumber));
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return mainTable.GetResolvedColumnName(column);
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			return mainTable.IndexOfColumn(columnName);
		}

		public override TableInfo TableInfo {
			get { return mainTable.TableInfo; }
		}

		public bool Equals(IRootTable other) {
			return this == other;
		}
	}
}
