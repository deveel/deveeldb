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
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Deveel.Data.Sql;
using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides a set of extension methods to <see cref="ITable"/>
	/// and <see cref="IMutableTable"/> objects.
	/// </summary>
	public static class TableQueryExtensions {
		private static DataObject MakeObject(this ITable table, int columnOffset, ISqlObject value) {
			if (columnOffset < 0 || columnOffset >= table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var columnType = table.TableInfo[columnOffset].ColumnType;
			return new DataObject(columnType, value);
		}

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex, DataObject value) {
			return table.GetIndex(columnIndex).SelectEqual(value);
		}

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex, ISqlObject value) {
			return table.SelectEqual(columnIndex, table.MakeObject(columnIndex, value));
		}

		public static IEnumerable<int> SelectNotEqual(this ITable table, int columnOffset, DataObject value) {
			return table.GetIndex(columnOffset).SelectNotEqual(value);
		}

		public static IEnumerable<int> SelectNotEqual(this ITable table, int columnOffset, ISqlObject value) {
			return table.SelectNotEqual(columnOffset, table.MakeObject(columnOffset, value));
		} 

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex1, DataObject value1, int columnIndex2, DataObject value2) {
			var result = new List<int>();

			var index1 = table.GetIndex(columnIndex1).SelectEqual(value1);
			foreach (var rowIndex in index1) {
				var tableValue = table.GetValue(rowIndex, columnIndex2);
				if (tableValue.IsEqualTo(value2))
					result.Add(rowIndex);
			}

			return result;
		}

		public static IEnumerable<int> SelectGreater(this ITable table, int columnOffset, DataObject value) {
			return table.GetIndex(columnOffset).SelectGreater(value);
		}

		public static IEnumerable<int> SelectGreater(this ITable table, int columnOffset, ISqlObject value) {
			return table.SelectGreater(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectGreaterOrEqual(this ITable table, int columnOffset, DataObject value) {
			return table.GetIndex(columnOffset).SelectGreaterOrEqual(value);
		}

		public static IEnumerable<int> SelectGreaterOrEqual(this ITable table, int columnOffset, ISqlObject value) {
			return table.SelectGreaterOrEqual(columnOffset, table.MakeObject(columnOffset, value));
		} 

		public static IEnumerable<int> SelectLess(this ITable table, int columnOffset, DataObject value) {
			return table.GetIndex(columnOffset).SelectLess(value);
		}

		public static IEnumerable<int> SelectLess(this ITable table, int columnOffset, ISqlObject value) {
			return table.SelectLess(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectLessOrEqual(this ITable table, int columnOffset, DataObject value) {
			return table.GetIndex(columnOffset).SelectLessOrEqual(value);
		}

		public static IEnumerable<int> SelectLessOrEqual(this ITable table, int columnOffset, ISqlObject value) {
			return table.SelectLessOrEqual(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectAll(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectAll();
		} 

		public static bool Exists(this ITable table, int columnOffset, DataObject value) {
			return table.SelectEqual(columnOffset, value).Any();
		}

		public static bool Exists(this ITable table, int columnOffset1, DataObject value1, int columnOffset2, DataObject value2) {
			return table.SelectEqual(columnOffset1, value1, columnOffset2, value2).Any();
		}

		public static IEnumerable<int> SelectRows(this ITable table, int column, BinaryOperator expressionType, DataObject value) {
			// If the cell is of an incompatible type, return no results,
			var colType = table.TableInfo[column].ColumnType;
			if (!value.Type.IsComparable(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			// Get the selectable scheme for this column
			var index = table.GetIndex(column);

			// If the operator is a standard operator, use the interned SelectableScheme
			// methods.
			if (expressionType.OperatorType == BinaryOperatorType.Equal)
				return index.SelectEqual(value);
			if (expressionType.OperatorType == BinaryOperatorType.NotEqual)
				return index.SelectNotEqual(value);
			if (expressionType.OperatorType == BinaryOperatorType.GreaterThan)
				return index.SelectGreater(value);
			if (expressionType.OperatorType == BinaryOperatorType.SmallerThan)
				return index.SelectLess(value);
			if (expressionType.OperatorType == BinaryOperatorType.GreaterOrEqualThan)
				return index.SelectGreaterOrEqual(value);
			if (expressionType.OperatorType == BinaryOperatorType.SmallerOrEqualThan)
				return index.SelectLessOrEqual(value);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			var rangeSet = new IndexRangeSet();
			rangeSet = rangeSet.Intersect(expressionType, value);
			return index.SelectRange(rangeSet.ToArray());
		}

		public static IEnumerable<int> SelectRows(this ITable table,
			IVariableResolver resolver,
			IQueryContext context,
			SqlBinaryExpression expression) {

			if (!(table is IDbTable))
				throw new NotSupportedException();

			var dbTable = (IDbTable) table;

			var objRef = expression.Left as SqlReferenceExpression;
			if (objRef == null)
				throw new NotSupportedException();
			if (objRef.IsToVariable)
				throw new InvalidOperationException();

			var columnName = objRef.ReferenceName;

			var column = dbTable.FindColumn(columnName);
			if (column < 0)
				throw new InvalidOperationException();

			var reduced = expression.Right.Evaluate(context, resolver);
			if (reduced.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException();

			var value = ((SqlConstantExpression) reduced).Value;
			var binOperator = expression.BinaryOperator;

			return table.SelectRows(column, binOperator, value);
		}

		#region Order By

		public static ITable OrderByColumns(this ITable table, int[] columns) {
			// Sort by the column list.
			ITable resultTable = table;
			for (int i = columns.Length - 1; i >= 0; --i) {
				resultTable = resultTable.OrderByColumn(columns[i], true);
			}

			// A nice post condition to check on.
			if (resultTable.RowCount != table.RowCount)
				throw new ApplicationException("The final row count mismatches.");

			return table;
		}

		public static ITable OrderByColumn(this ITable table, int columnIndex, bool ascending) {
			if (table == null)
				return null;

			if (!(table is IDbTable))
				throw new NotSupportedException(String.Format("The table type {0} is not supported for this operation", table.GetType()));

			var rows = table.SelectAll(columnIndex);

			// Reverse the list if we are not ascending
			if (ascending == false)
				rows = rows.Reverse();

			return new VirtualTable((IDbTable)table, rows);
		}

		#endregion

		public static ITable ColumnMerge(this ITable table, ITable other) {
			if (table.RowCount != other.RowCount)
				throw new ApplicationException("Tables have different row counts.");

			if (!(table is IDbTable) ||
				!(other is IDbTable))
				throw new NotSupportedException();

			// Create the new VirtualTable with the joined tables.

			List<int> allRowSet = new List<int>();
			int rcount = table.RowCount;
			for (int i = 0; i < rcount; ++i) {
				allRowSet.Add(i);
			}

			var tabs = new IDbTable[] { (IDbTable) table, (IDbTable) other };
			var rowSets = new IList<int>[] { allRowSet, allRowSet };

			return new VirtualTable(tabs, rowSets);
		}

		public static Dictionary<string, ISqlObject> ToDictionary(this ITable table) {
			if (table.TableInfo.ColumnCount != 2)
				throw new NotSupportedException("Table must have two columns.");

			var map = new Dictionary<string, ISqlObject>();
			foreach (var row in table) {
				var key = row.GetValue(0);
				var value = row.GetValue(1);
				map[key.AsVarChar().Value.ToString()] = value.Value;
			}

			return map;
		}

		#region Join

		public static ITable Join(this ITable table, ITable otherTable, bool quick) {
			if (!(table is IDbTable) ||
				!(otherTable is IDbTable))
				throw new NotSupportedException();

			IDbTable outTable;

			if (quick) {
				// This implementation doesn't materialize the join
				outTable = new NaturallyJoinedTable((IDbTable) table, (IDbTable) otherTable);
			} else {
				var tabs = new IDbTable[] {(IDbTable) table, (IDbTable) otherTable};
				var rowSets = new IList<int>[2];

				// Optimized trivial case, if either table has zero rows then result of
				// join will contain zero rows also.
				if (table.RowCount == 0 || otherTable.RowCount == 0) {
					rowSets[0] = new List<int>(0);
					rowSets[1] = new List<int>(0);
				} else {
					// The natural join algorithm.
					List<int> thisRowSet = new List<int>();
					List<int> tableRowSet = new List<int>();

					// Get the set of all rows in the given table.
					List<int> tableSelectedSet = new List<int>();
					var e = otherTable.GetEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.Current.RowId.RowNumber;
						tableSelectedSet.Add(rowIndex);
					}

					int tableSelectedSetSize = tableSelectedSet.Count;

					// Join with the set of rows in this table.
					e = table.GetEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.Current.RowId.RowNumber;
						for (int i = 0; i < tableSelectedSetSize; ++i) {
							thisRowSet.Add(rowIndex);
						}
						tableRowSet.AddRange(tableSelectedSet);
					}

					// The row sets we are joining from each table.
					rowSets[0] = thisRowSet;
					rowSets[1] = tableRowSet;
				}

				// Create the new VirtualTable with the joined tables.
				outTable = new VirtualTable(tabs, rowSets);
			}

			return outTable;
		}

		public static ITable Join(this ITable table, ITable otherTable) {
			return table.Join(otherTable, true);
		}

		public static ITable SimpleJoin(this ITable thisTable, IQueryContext context, ITable table, SqlBinaryExpression binary) {
			if (!(thisTable is IDbTable))
				throw new NotSupportedException();

			var thisDbTable = (IDbTable) thisTable;
			var dbTable = (IDbTable) table;

			var objRef = binary.Left as SqlReferenceExpression;
			if (objRef == null)
				throw new ArgumentException();
			if (objRef.IsToVariable)
				throw new ArgumentException();

			// Find the row with the name given in the condition.
			int lhsColumn = thisDbTable.FindColumn(objRef.ReferenceName);

			if (lhsColumn == -1)
				throw new Exception("Unable to find the LHS column specified in the condition: " + objRef.ReferenceName);

			// Create a variable resolver that can resolve columns in the destination
			// table.
			var resolver = dbTable.GetVariableResolver();

			// The join algorithm.  It steps through the RHS expression, selecting the
			// cells that match the relation from the LHS table (this table).

			var thisRowSet = new List<int>();
			var tableRowSet = new List<int>();

			var e = table.GetEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.Current.RowId.RowNumber;
				resolver.AssignSetId(rowIndex);

				// Select all the rows in this table that match the joining condition.
				var selectedSet = thisTable.SelectRows(resolver, context, binary);

				var selectList = selectedSet.ToList();

				var size = selectList.Count;
				// Include in the set.
				for (int i = 0; i < size; i++) {
					tableRowSet.Add(rowIndex);
				}

				thisRowSet.AddRange(selectList);
			}

			// Create the new VirtualTable with the joined tables.

			var tabs = new[] {(IDbTable) thisTable, (IDbTable)table};
			var rowSets = new[] {thisRowSet.AsEnumerable(), tableRowSet.AsEnumerable()};

			return new VirtualTable(tabs, rowSets);
		}

		#endregion

		public static bool RemoveRow(this IMutableTable table, int rowIndex) {
			return table.RemoveRow(new RowId(table.TableInfo.Id, rowIndex));
		}
	}
}