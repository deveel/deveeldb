// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Caching;
using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	class FunctionTable : BaseDataTable {
		private readonly IQueryContext context;
		private readonly ITableVariableResolver varResolver;
		private readonly TableInfo funTableInfo;
		private bool wholeTableIsSimpleEnum;
		private readonly int uniqueId;
		private readonly byte[] expInfo;
		private readonly SqlExpression[] expList;
		private readonly int rowCount;

		private IList<int> groupLinks;
		private IList<int> groupLookup;
		private TableGroupResolver groupResolver;
		private bool wholeTableAsGroup;
		private IList<int> wholeTableGroup;
		private int wholeTableGroupSize;

		private static readonly ObjectName FunctionTableName = new ObjectName(null, "FUNCTIONTABLE");

		private static int uniqueKeySeq = 0;

		public FunctionTable(SqlExpression[] functionList, string[] columnNames, IQueryContext queryContext)
			: this(queryContext.Session.Database.SingleRowTable, functionList, columnNames, queryContext) {
		}

		public FunctionTable(ITable table, SqlExpression[] functionList, string[] columnNames, IQueryContext queryContext)
			: base(queryContext.DatabaseContext()) {
			// Make sure we are synchronized over the class.
			lock (typeof(FunctionTable)) {
				uniqueId = uniqueKeySeq;
				++uniqueKeySeq;
			}

			uniqueId = (uniqueId & 0x0FFFFFFF) | 0x010000000;

			context = queryContext;

			ReferenceTable = table;
			varResolver = table.GetVariableResolver();
			varResolver = varResolver.ForRow(0);

			// Create a DataTableInfo object for this function table.
			funTableInfo = new TableInfo(FunctionTableName);

			expList = new SqlExpression[functionList.Length];
			expInfo = new byte[functionList.Length];

			// Create a new DataColumnInfo for each expression, and work out if the
			// expression is simple or not.
			for (int i = 0; i < functionList.Length; ++i) {
				var expr = functionList[i];
				// Examine the expression and determine if it is simple or not
				if (expr.IsConstant() && !expr.HasAggregate(context)) {
					// If expression is a constant, solve it
					var result = expr.Evaluate(context, null);
					if (result.ExpressionType != SqlExpressionType.Constant)
						throw new InvalidOperationException();

					expr = result;
					expList[i] = expr;
					expInfo[i] = 1;
				} else {
					// Otherwise must be dynamic
					expList[i] = expr;
					expInfo[i] = 0;
				}

				// Make the column info
				funTableInfo.AddColumn(columnNames[i], expr.ReturnType(context, varResolver));
			}

			// Make sure the table info isn't changed from this point on.
			funTableInfo = funTableInfo.AsReadOnly();

			// routine tables are the size of the referring table.
			rowCount = table.RowCount;

			// Set schemes to 'blind search'.
			SetupIndexes(DefaultIndexTypes.BlindSearch);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		public ITable ReferenceTable { get; private set; }

		public override TableInfo TableInfo {
			get { return funTableInfo; }
		}

		public override int RowCount {
			get { return rowCount; }
		}

		public override void LockRoot(int lockKey) {
			// We Lock the reference table.
			// NOTE: This cause the reference table to Lock twice when we use the
			//  'MergeWithReference' method.  While this isn't perfect behaviour, it
			//  means if 'MergeWithReference' isn't used, we still maintain a safe
			//  level of locking.
			ReferenceTable.LockRoot(lockKey);
		}

		public override void UnlockRoot(int lockKey) {
			// We unlock the reference table.
			// NOTE: This cause the reference table to unlock twice when we use the
			//  'MergeWithReference' method.  While this isn't perfect behaviour, it
			//  means if 'MergeWithReference' isn't used, we still maintain a safe
			//  level of locking.
			ReferenceTable.UnlockRoot(lockKey);
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			// [ FUNCTION TABLE CACHING NOW USES THE GLOBAL CELL CACHING MECHANISM ]
			// Check if in the cache,
			var cache = DatabaseContext.TableCellCache();

			// Is the column worth caching, and is caching enabled?
			if (expInfo[columnOffset] == 0 && cache != null) {
				DataObject cell;
				if (cache.TryGetValue(uniqueId, (int)rowNumber, columnOffset, out cell))
					// In the cache so return the cell.
					return cell;

				// Not in the cache so calculate the value and write it in the cache.
				return CalcValue((int)rowNumber, columnOffset, cache);
			}

			// Caching is not enabled
			return CalcValue((int)rowNumber, columnOffset, null);

		}

		private DataObject CalcValue(int row, int column, ITableCellCache cache) {
			var resolver = varResolver.ForRow(row);

			if (groupResolver != null) {
				groupResolver.SetUpGroupForRow(row);
			}

			var expr = expList[column];
			var exp = expr.Evaluate(context, resolver, groupResolver);
			if (exp.ExpressionType != SqlExpressionType.Constant)
				throw new ArgumentException();

			var value = ((SqlConstantExpression) exp).Value;
			if (cache != null)
				cache.Set(uniqueId, row, column, value);

			return value;
		}

		private int GetRowGroup(int rowIndex) {
			return groupLookup[rowIndex];
		}

		private int GetGroupSize(int groupNumber) {
			int groupSize = 1;
			int i = groupLinks[groupNumber];
			while ((i & 0x040000000) == 0) {
				++groupSize;
				++groupNumber;
				i = groupLinks[groupNumber];
			}
			return groupSize;
		}

		private IList<int> GetGroupRows(int groupNumber) {
			var rows = new List<int>();
			var row = groupLinks[groupNumber];

			while ((row & 0x040000000) == 0) {
				rows.Add(row);
				++groupNumber;
				row = groupLinks[groupNumber];
			}

			rows.Add(row & 0x03FFFFFFF);
			return rows;
		}

		private IList<int> GetTopRowsFromEachGroup() {
			var extractRows = new List<int>();
			var size = groupLinks.Count;
			var take = true;

			for (int i = 0; i < size; ++i) {
				int r = groupLinks[i];
				if (take)
					extractRows.Add(r & 0x03FFFFFFF);

				take = (r & 0x040000000) != 0;
			}

			return extractRows;
		}

		private IList<int> GetMaxFromEachGroup(int colNum) {
			var refTab = ReferenceTable;

			var extractRows = new List<int>();
			var size = groupLinks.Count;

			int toTakeInGroup = -1;
			DataObject max = null;

			for (int i = 0; i < size; ++i) {
				int row = groupLinks[i];

				int actRIndex = row & 0x03FFFFFFF;
				var cell = refTab.GetValue(actRIndex, colNum);

				if (max == null || cell.CompareTo(max) > 0) {
					max = cell;
					toTakeInGroup = actRIndex;
				}

				if ((row & 0x040000000) != 0) {
					extractRows.Add(toTakeInGroup);
					max = null;
				}
			}

			return extractRows;
		}

		public ITable MergeWith(ObjectName maxColumn) {
			var table = ReferenceTable;

			IList<int> rowList;

			if (wholeTableAsGroup) {
				// Whole table is group, so take top entry of table.

				rowList = new List<int>(1);
				var rowEnum = table.GetEnumerator();
				if (rowEnum.MoveNext()) {
					rowList.Add(rowEnum.Current.RowId.RowNumber);
				} else {
					// MAJOR HACK: If the referencing table has no elements then we choose
					//   an arbitary index from the reference table to merge so we have
					//   at least one element in the table.
					//   This is to fix the 'SELECT COUNT(*) FROM empty_table' bug.
					rowList.Add(Int32.MaxValue - 1);
				}
			} else if (table.RowCount == 0) {
				rowList = new List<int>(0);
			} else if (groupLinks != null) {
				// If we are grouping, reduce down to only include one row from each
				// group.
				if (maxColumn == null) {
					rowList = GetTopRowsFromEachGroup();
				} else {
					var colNum = ReferenceTable.FindColumn(maxColumn);
					rowList = GetMaxFromEachGroup(colNum);
				}
			} else {
				// OPTIMIZATION: This should be optimized.  It should be fairly trivial
				//   to generate a Table implementation that efficiently merges this
				//   function table with the reference table.

				// This means there is no grouping, so merge with entire table,
				int rowCount = table.RowCount;
				rowList = new List<int>(rowCount);
				var en = table.GetEnumerator();
				while (en.MoveNext()) {
					rowList.Add(en.Current.RowId.RowNumber);
				}
			}

			// Create a virtual table that's the new group table merged with the
			// functions in this...

			var tabs = new [] { table, this };
			var rowSets = new[] { rowList, rowList };

			return new VirtualTable(tabs, rowSets);
		}

		public FunctionTable AsGroup() {
			// TODO: create a new table ...
			wholeTableAsGroup = true;

			wholeTableGroupSize = ReferenceTable.RowCount;

			// Set up 'whole_table_group' to the list of all rows in the reference
			// table.
			var en = ReferenceTable.GetEnumerator();
			wholeTableIsSimpleEnum = en is SimpleRowEnumerator;
			if (!wholeTableIsSimpleEnum) {
				wholeTableGroup = new List<int>(ReferenceTable.RowCount);
				while (en.MoveNext()) {
					wholeTableGroup.Add(en.Current.RowId.RowNumber);
				}
			}

			// Set up a group resolver for this method.
			groupResolver = new TableGroupResolver(this);
			return this;
		}

		public FunctionTable CreateGroupMatrix(ObjectName[] columns) {
			// If we have zero rows, then don't bother creating the matrix.
			if (RowCount <= 0 || columns.Length <= 0)
				return this;

			var rootTable = ReferenceTable;
			int rowCount = rootTable.RowCount;
			int[] colLookup = new int[columns.Length];
			for (int i = columns.Length - 1; i >= 0; --i) {
				colLookup[i] = rootTable.IndexOfColumn(columns[i]);
			}

			var rowList = rootTable.OrderedRows(colLookup).ToList();

			// 'row_list' now contains rows in this table sorted by the columns to
			// group by.

			// This algorithm will generate two lists.  The group_lookup list maps
			// from rows in this table to the group number the row belongs in.  The
			// group number can be used as an index to the 'group_links' list that
			// contains consequtive links to each row in the group until -1 is reached
			// indicating the end of the group;

			groupLookup = new List<int>(rowCount);
			groupLinks = new List<int>(rowCount);
			int currentGroup = 0;
			int previousRow = -1;
			for (int i = 0; i < rowCount; i++) {
				var rowIndex = rowList[i];

				if (previousRow != -1) {
					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < colLookup.Length && equal; ++n) {
						var c1 = rootTable.GetValue(rowIndex, colLookup[n]);
						var c2 = rootTable.GetValue(previousRow, colLookup[n]);
						equal = (c1.CompareTo(c2) == 0);
					}

					if (!equal) {
						// If end of group, set bit 15
						groupLinks.Add(previousRow | 0x040000000);
						currentGroup = groupLinks.Count;
					} else {
						groupLinks.Add(previousRow);
					}
				}

				// groupLookup.Insert(row_index, current_group);
				PlaceAt(groupLookup, rowIndex, currentGroup);

				previousRow = rowIndex;
			}

			// Add the final row.
			groupLinks.Add(previousRow | 0x040000000);

			// Set up a group resolver for this method.
			groupResolver = new TableGroupResolver(this);

			return this;
		}

		private static void PlaceAt(IList<int> list, int index, int value) {
			while (index > list.Count) {
				list.Add(0);
			}

			list.Insert(index, value);
		}

		public static ITable ResultTable(IQueryContext context, SqlExpression expression) {
			var exp = new [] { expression };
			var names = new[] { "result" };
			var table = new FunctionTable(exp, names, context);

			return new SubsetColumnTable(table, new int[0], new []{new ObjectName("result") });
		}

		public static ITable ResultTable(IQueryContext context, DataObject value) {
			return ResultTable(context, SqlExpression.Constant(value));
		}

		public static ITable ResultTable(IQueryContext context, int value) {
			return ResultTable(context, DataObject.Integer(value));
		}

		#region TableGroupResolver

		class TableGroupResolver : IGroupResolver {
			private IList<int> group;
			private ITableVariableResolver groupVarResolver;

			public TableGroupResolver(FunctionTable table) {
				Table = table;
				GroupId = -1;
			}

			public FunctionTable Table { get; private set; }

			public int GroupId { get; private set; }

			public int Count {
				get {
					if (GroupId == -2)
						return Table.wholeTableGroupSize;
					if (group != null)
						return group.Count;

					return Table.GetGroupSize(GroupId);
				}
			}

			private void EnsureGroup() {
				if (group == null) {
					if (GroupId == -2) {
						group = Table.wholeTableGroup;
					} else {
						group = Table.GetGroupRows(GroupId);
					}
				}
			}

			public DataObject Resolve(ObjectName variable, int setIndex) {
				int colIndex = Table.ReferenceTable.FindColumn(variable);
				if (colIndex == -1)
					throw new InvalidOperationException(String.Format("Column {0} not found in table {1}.", variable, Table.TableName));

				EnsureGroup();

				int rowIndex = setIndex;
				if (group != null)
					rowIndex = group[setIndex];

				return Table.ReferenceTable.GetValue(rowIndex, colIndex);
			}

			private ITableVariableResolver CreateVariableResolver() {
				if (groupVarResolver == null)
					groupVarResolver = new GroupVariableResolver(this);

				return groupVarResolver;
			}

			public IVariableResolver GetVariableResolver(int setIndex) {
				var resolver = CreateVariableResolver();
				resolver = resolver.ForRow(setIndex);
				return resolver;
			}

			public void SetUpGroupForRow(int rowIndex) {
				if (Table.wholeTableAsGroup) {
					if (GroupId != -2) {
						GroupId = -2;
						group = null;
					}
				} else {
					int g = Table.GetRowGroup(rowIndex);
					if (g != GroupId) {
						GroupId = g;
						group = null;
					}
				}
			}

			#region GroupVariableResolver

			class GroupVariableResolver : ITableVariableResolver {
				private readonly TableGroupResolver groupResolver;
				private readonly int rowIndex;

				public GroupVariableResolver(TableGroupResolver groupResolver) 
					: this(groupResolver, -1) {
				}

				public GroupVariableResolver(TableGroupResolver groupResolver, int rowIndex) {
					this.groupResolver = groupResolver;
					this.rowIndex = rowIndex;
				}

				public DataObject Resolve(ObjectName variable) {
					if (rowIndex < 0)
						throw new InvalidOperationException();

					return groupResolver.Resolve(variable, rowIndex);
				}

				public DataType ReturnType(ObjectName variable) {
					var columnOffset = groupResolver.Table.FindColumn(variable);
					if (columnOffset < 0)
						throw new InvalidOperationException(String.Format("Cannot find column {0} in table {1}", variable,
							groupResolver.Table.TableName));

					return groupResolver.Table.TableInfo[columnOffset].ColumnType;
				}

				public ITableVariableResolver ForRow(int rowNum) {
					return new GroupVariableResolver(groupResolver, rowNum);
				}
			}

			#endregion
		}

		#endregion
	}
}
