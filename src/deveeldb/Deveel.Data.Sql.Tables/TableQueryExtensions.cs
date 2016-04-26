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
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data;
using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Text;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// Provides a set of extension methods to <see cref="ITable"/>
	/// and <see cref="IMutableTable"/> objects.
	/// </summary>
	public static class TableQueryExtensions {
		#region Get Value

		public static Field GetValue(this ITable table, int rowIndex, ObjectName columnName) {
			return table.GetValue(rowIndex, table.IndexOfColumn(columnName));
		}

		public static Field GetValue(this ITable table, int rowIndex, string columnName) {
			return table.GetValue(rowIndex, table.ResolveColumnName(columnName));
		}

		public static Field GetLastValue(this ITable table, int columnOffset) {
			var rows = table.SelectLast(columnOffset).ToList();
			return rows.Count > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static Field GetLastValue(this ITable table, string columnName) {
			return table.GetLastValue(table.IndexOfColumn(columnName));
		}

		public static Field[] GetLastValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetLastValue(columnOffsets[0])};
		}

		public static Field GetFirstValue(this ITable table, int columnOffset) {
			var rows = table.SelectFirst(columnOffset).ToList();
			return rows.Count > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static Field GetFirstValue(this ITable table, string columnName) {
			return table.GetFirstValue(table.IndexOfColumn(columnName));
		}

		public static Field[] GetFirstValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetFirstValue(columnOffsets[0])};
		}

		public static Field GetSingleValue(this ITable table, int columnOffset) {
			IList<int> rows = table.SelectFirst(columnOffset).ToList();
			int sz = rows.Count;
			return sz == table.RowCount && sz > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static Field GetSingleValue(this ITable table, string columnName) {
			return table.GetSingleValue(table.IndexOfColumn(columnName));
		}

		public static Field[] GetSingleValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetSingleValue(columnOffsets[0])};
		}

		#endregion

		#region Get Row

		public static Row GetRow(this ITable table, int rowNumber) {
			return new Row(table, new RowId(table.TableInfo.Id, rowNumber));
		}

		#endregion

		public static IEnumerable<int> FindKeys(this ITable table, int[] columnOffsets, Field[] keyValue) {
			int keySize = keyValue.Length;

			// Now command table 2 to determine if the key values are present.
			// Use index scan on first key.
			var columnIndex = table.GetIndex(columnOffsets[0]);
			var list = columnIndex.SelectEqual(keyValue[0]).ToList();

			if (keySize <= 1)
				return list;

			// Full scan for the rest of the columns
			int sz = list.Count;

			// For each element of the list
			for (int i = sz - 1; i >= 0; --i) {
				int rIndex = list[i];
				// For each key in the column list
				for (int c = 1; c < keySize; ++c) {
					int columnOffset = columnOffsets[c];
					var columnValue = keyValue[c];
					if (columnValue.CompareTo(table.GetValue(rIndex, columnOffset)) != 0) {
						// If any values in the key are not equal set this flag to false
						// and remove the index from the list.
						list.RemoveAt(i);
						// Break the for loop
						break;
					}
				}
			}

			return list;
		}

		private static Field MakeObject(this ITable table, int columnOffset, Objects.ISqlObject value) {
			if (columnOffset < 0 || columnOffset >= table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var columnType = table.TableInfo[columnOffset].ColumnType;
			return new Field(columnType, value);
		}

		public static int IndexOfColumn(this ITable table, string columnName) {
			if (table is IQueryTable)
				return ((IQueryTable) table).FindColumn(ObjectName.Parse(columnName));

			return table.TableInfo.IndexOfColumn(columnName);
		}

		public static int IndexOfColumn(this ITable table, ObjectName columnName) {
			if (table is IQueryTable)
				return ((IQueryTable) table).FindColumn(columnName);

			if (columnName.Parent != null &&
			    !columnName.Parent.Equals(table.TableInfo.TableName))
				return -1;

			return table.TableInfo.IndexOfColumn(columnName);
		}

		#region Select Rows

		public static IEnumerable<int> SelectRowsEqual(this ITable table, int columnIndex, Field value) {
			return table.GetIndex(columnIndex).SelectEqual(value);
		}

		public static IEnumerable<int> SelectRowsEqual(this ITable table, string columnName, Field value) {
			return table.SelectRowsEqual(table.IndexOfColumn(columnName), value);
		}

		public static IEnumerable<int> SelectNotEqual(this ITable table, int columnOffset, Field value) {
			return table.GetIndex(columnOffset).SelectNotEqual(value);
		}

		public static IEnumerable<int> SelectNotEqual(this ITable table, int columnOffset, Objects.ISqlObject value) {
			return table.SelectNotEqual(columnOffset, table.MakeObject(columnOffset, value));
		} 

		public static IEnumerable<int> SelectRowsEqual(this ITable table, int columnIndex1, Field value1, int columnIndex2, Field value2) {
			var result = new List<int>();

			var index1 = table.GetIndex(columnIndex1).SelectEqual(value1);
			foreach (var rowIndex in index1) {
				var tableValue = table.GetValue(rowIndex, columnIndex2);
				if (tableValue.IsEqualTo(value2))
					result.Add(rowIndex);
			}

			return result;
		}

		public static IEnumerable<int> SelectRowsRange(this ITable table, int column, IndexRange[] ranges) {
			return table.GetIndex(column).SelectRange(ranges);
		}

		public static IEnumerable<int> SelectRowsGreater(this ITable table, int columnOffset, Field value) {
			return table.GetIndex(columnOffset).SelectGreater(value);
		}

		public static IEnumerable<int> SelectRowsGreater(this ITable table, int columnOffset, Objects.ISqlObject value) {
			return table.SelectRowsGreater(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectRowsGreaterOrEqual(this ITable table, int columnOffset, Field value) {
			return table.GetIndex(columnOffset).SelectGreaterOrEqual(value);
		}

		public static IEnumerable<int> SelectRowsGreaterOrEqual(this ITable table, int columnOffset, Objects.ISqlObject value) {
			return table.SelectRowsGreaterOrEqual(columnOffset, table.MakeObject(columnOffset, value));
		} 

		public static IEnumerable<int> SelecRowsLess(this ITable table, int columnOffset, Field value) {
			return table.GetIndex(columnOffset).SelectLess(value);
		}

		public static IEnumerable<int> SelecRowsLess(this ITable table, int columnOffset, Objects.ISqlObject value) {
			return table.SelecRowsLess(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectRowsLessOrEqual(this ITable table, int columnOffset, Field value) {
			return table.GetIndex(columnOffset).SelectLessOrEqual(value);
		}

		public static IEnumerable<int> SelectRowsLessOrEqual(this ITable table, int columnOffset, Objects.ISqlObject value) {
			return table.SelectRowsLessOrEqual(columnOffset, table.MakeObject(columnOffset, value));
		}

		public static IEnumerable<int> SelectAllRows(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectAll();
		}

		public static IEnumerable<int> SelectAllRows(this ITable table) {
			return table.Select(x => x.RowId.RowNumber);
		}

		public static IEnumerable<int> SelectLast(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectLast();
		}

		public static IEnumerable<int> SelectFirst(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectFirst();
		}

		public static IEnumerable<int> SelectRows(this ITable table,
			IVariableResolver resolver,
			IRequest context,
			SqlBinaryExpression expression) {

			var objRef = expression.Left as SqlReferenceExpression;
			if (objRef == null)
				throw new NotSupportedException();

			var columnName = objRef.ReferenceName;

			var column = table.FindColumn(columnName);
			if (column < 0)
				throw new InvalidOperationException();

			var reduced = expression.Right.Evaluate(context, resolver);
			if (reduced.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException();

			var value = ((SqlConstantExpression) reduced).Value;
			var binOperator = expression.ExpressionType;

			return table.SelectRows(column, binOperator, value);
		}

		public static IEnumerable<int> SelectRows(this ITable table, int[] columnOffsets, SqlExpressionType op,
			Field[] values) {
			if (columnOffsets.Length > 1)
				throw new NotSupportedException("Multi-column selects not supported yet.");

			return SelectRows(table, columnOffsets[0], op, values[0]);
		}

		public static IEnumerable<int> SelectRowsBetween(this ITable table, int column, Field minCell, Field maxCell) {
			// Check all the tables are comparable
			var colType = table.TableInfo[column].ColumnType;
			if (!minCell.Type.IsComparable(colType) ||
			    !maxCell.Type.IsComparable(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			return table.GetIndex(column).SelectBetween(minCell, maxCell);
		}

		public static IEnumerable<int> SelectRows(this ITable table, int column, SqlExpressionType op, Field value) {
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
			if (op == SqlExpressionType.Equal)
				return index.SelectEqual(value);
			if (op == SqlExpressionType.NotEqual)
				return index.SelectNotEqual(value);
			if (op == SqlExpressionType.GreaterThan)
				return index.SelectGreater(value);
			if (op == SqlExpressionType.SmallerThan)
				return index.SelectLess(value);
			if (op == SqlExpressionType.GreaterOrEqualThan)
				return index.SelectGreaterOrEqual(value);
			if (op == SqlExpressionType.SmallerOrEqualThan)
				return index.SelectLessOrEqual(value);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			var rangeSet = new IndexRangeSet();
			rangeSet = rangeSet.Intersect(op, value);
			return index.SelectRange(rangeSet.ToArray());
		}

		public static IEnumerable<int> Search(this ITable table, int column, string pattern) {
			return table.Search(column, pattern, '\\');
		}

		public static IEnumerable<int> Search(this ITable table, int column, string pattern, char escapeChar) {
			var colType = table.TableInfo[column].ColumnType;

			// If the column type is not a string type then report an error.
			if (!(colType is StringType))
				throw new InvalidOperationException("Unable to perform a pattern search on a non-String type column.");

			// First handle the case that the column has an index that supports text search
			var index = table.GetIndex(column);
			if (index != null && index.HandlesTextSearch)
				return index.SelectLike(Field.String(pattern));

			var colStringType = (StringType)colType;

			// ---------- Pre Search ----------

			// First perform a 'pre-search' on the head of the pattern.  Note that
			// there may be no head in which case the entire column is searched which
			// has more potential to be expensive than if there is a head.

			StringBuilder prePattern = new StringBuilder();
			int i = 0;
			bool finished = i >= pattern.Length;
			bool lastIsEscape = false;

			while (!finished) {
				char c = pattern[i];
				if (lastIsEscape) {
					lastIsEscape = true;
					prePattern.Append(c);
				} else if (c == escapeChar) {
					lastIsEscape = true;
				} else if (!PatternSearch.IsWildCard(c)) {
					prePattern.Append(c);

					++i;
					if (i >= pattern.Length) {
						finished = true;
					}

				} else {
					finished = true;
				}
			}

			// This is set with the remaining search.
			string postPattern;

			// This is our initial search row set.  In the second stage, rows are
			// eliminated from this vector.
			IEnumerable<int> searchCase;

			if (i >= pattern.Length) {
				// If the pattern has no 'wildcards' then just perform an EQUALS
				// operation on the column and return the results.

				var cell = new Field(colType, new SqlString(pattern));
				return SelectRows(table, column, SqlExpressionType.Equal, cell);
			}

			if (prePattern.Length == 0 ||
			    colStringType.Locale != null) {

				// No pre-pattern easy search :-(.  This is either because there is no
				// pre pattern (it starts with a wild-card) or the locale of the string
				// is non-lexicographical.  In either case, we need to select all from
				// the column and brute force the search space.

				searchCase = table.SelectAllRows(column);
				postPattern = pattern;
			} else {

				// Criteria met: There is a pre_pattern, and the column locale is
				// lexicographical.

				// Great, we can do an upper and lower bound search on our pre-search
				// set.  eg. search between 'Geoff' and 'Geofg' or 'Geoff ' and
				// 'Geoff\33'

				var lowerBounds = prePattern.ToString();
				int nextChar = prePattern[i - 1] + 1;
				prePattern[i - 1] = (char)nextChar;
				var upperBounds = prePattern.ToString();

				postPattern = pattern.Substring(i);

				var cellLower = new Field(colType, new SqlString(lowerBounds));
				var cellUpper = new Field(colType, new SqlString(upperBounds));

				// Select rows between these two points.

				searchCase = table.SelectRowsBetween(column, cellLower, cellUpper);
			}

			// ---------- Post search ----------

			int preIndex = i;

			// Now eliminate from our 'search_case' any cells that don't match our
			// search pattern.
			// Note that by this point 'post_pattern' will start with a wild card.
			// This follows the specification for the 'PatternMatch' method.
			// EFFICIENCY: This is a brute force iterative search.  Perhaps there is
			//   a faster way of handling this?

			var iList = new BlockIndex<int>(searchCase);
			var enumerator = iList.GetEnumerator(0, iList.Count - 1);

			while (enumerator.MoveNext()) {
				// Get the expression (the contents of the cell at the given column, row)

				bool patternMatches = false;
				var cell = table.GetValue(enumerator.Current, column);
				// Null values doesn't match with anything
				if (!cell.IsNull) {
					string expression = cell.AsVarChar().Value.ToString();
					// We must remove the head of the string, which has already been
					// found from the pre-search section.
					expression = expression.Substring(preIndex);
					patternMatches = PatternSearch.PatternMatch(postPattern, expression, escapeChar);
				}
				if (!patternMatches) {
					// If pattern does not match then remove this row from the search.
					enumerator.Remove();
				}
			}

			return iList.ToList();
		}

		#endregion

		#region Select

		public static ITable SelectEqual(this ITable table, int columnIndex, Field value) {
			return table.AsVirtual(() => table.SelectRowsEqual(columnIndex, value));
		}


		public static ITable SelectEqual(this ITable table, string columnName, Field value) {
			return table.AsVirtual(() => table.SelectRowsEqual(columnName, value));
		}

		public static ITable SelectAll(this ITable table, int columnOffset) {
			return table.AsVirtual(() => table.SelectAllRows(columnOffset));
		}

		public static ITable Select(this ITable table, IRequest context, SqlExpression expression) {
			if (expression is SqlBinaryExpression) {
				var binary = (SqlBinaryExpression)expression;

				// Perform the pattern search expression on the table.
				// Split the expression,
				var leftRef = binary.Left.AsReferenceName();
				if (leftRef != null)
					// LHS is a simple variable so do a simple select
					return table.SimpleSelect(context, leftRef, binary.ExpressionType, binary.Right);
			}

			// LHS must be a constant so we can just evaluate the expression
			// and see if we get true, false, null, etc.
			var v = expression.EvaluateToConstant(context, null);

			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v == false)
				return table.EmptySelect();

			return table;
		}

		public static ITable SimpleSelect(this ITable table, IRequest context, ObjectName columnName, SqlExpressionType op, SqlExpression exp) {
			// Find the row with the name given in the condition.
			int column = table.FindColumn(columnName);

			if (column == -1)
				throw new ArgumentException(String.Format("Unable to find the column {0} in the condition.", columnName.Name));

			// If we are doing a sub-query search
			if (exp is SqlQuantifiedExpression) {
				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				var quantified = (SqlQuantifiedExpression) exp;
				if (!quantified.IsArrayValue &&
				    !quantified.IsTupleValue)
					throw new ArgumentException();

				IEnumerable<SqlExpression> list;
				bool isAll = quantified.ExpressionType == SqlExpressionType.All;

				if (quantified.IsArrayValue) {
					var tob = ((SqlConstantExpression) quantified.ValueExpression).Value;
					if (tob.Type is ArrayType) {
						var array = (SqlArray) tob.Value;
						list = array;
					} else {
						throw new Exception("The right side of a sub-query operator must be a tuple or a sub-query.");
					}
				} else {
					list = ((SqlTupleExpression) quantified.ValueExpression).Expressions;
				}

				// Construct a temporary table with a single column that we are
				// comparing to.
				var col = table.TableInfo[column];
				var ttable = TemporaryTable.SingleColumnTable(table.Context, col.ColumnName, col.ColumnType);

				foreach (var expression in list) {
					var rowNum = ttable.NewRow();

					var evalExp = (SqlConstantExpression)expression.Evaluate(context, null, null);
					ttable.SetValue(rowNum, 0, evalExp.Value);
				}

				ttable.BuildIndexes();

				// Perform the any/all sub-query on the constant table.

				return table.SelectAnyAllNonCorrelated(new[] { columnName }, op, isAll, ttable);
			}

			{
				if (!exp.IsConstant())
					throw new ArgumentException("The search expression is not constant.");

				var evalExp = exp.Evaluate(context, null);
				if (evalExp.ExpressionType != SqlExpressionType.Constant)
					throw new InvalidOperationException();

				var value = ((SqlConstantExpression) evalExp).Value;

				IEnumerable<int> rows;

				if (op == SqlExpressionType.Like ||
				    op == SqlExpressionType.NotLike
					/* TODO: ||
				op.IsOfType(BinaryOperatorType.Regex)*/) {

					/*
				TODO:
				if (op.IsOfType(BinaryOperatorType.Regex)) {
					rows = SelectFromRegex(column, op, value);
				} else {
				 */
					rows = table.SelectFromPattern(column, op, value);
				} else {

					// Is the column we are searching on indexable?
					var colInfo = table.TableInfo[column];
					if (!colInfo.IsIndexable)
						throw new InvalidOperationException(String.Format("Column {0} os type {1} cannot be searched.", colInfo.ColumnName,
							colInfo.ColumnType));

					rows = table.SelectRows(column, op, value);
				}

				return new VirtualTable(table, rows.ToArray()) {SortColumn = column};
			}
		}

		public static ITable ExhaustiveSelect(this ITable table, IRequest context, SqlExpression expression) {
			var result = table;

			// Exit early if there's nothing in the table to select from
			int rowCount = table.RowCount;
			if (rowCount > 0) {
				var tableResolver = table.GetVariableResolver();
				List<int> selectedSet = new List<int>(rowCount);

				foreach (var row in table) {
					int rowIndex = row.RowId.RowNumber;

					var rowResolver = tableResolver.ForRow(rowIndex);

					// Resolve expression into a constant.
					var exp = expression.Evaluate(context, rowResolver);
					if (exp.ExpressionType != SqlExpressionType.Constant)
						throw new NotSupportedException();

					var value = ((SqlConstantExpression) exp).Value;
					// If resolved to true then include in the selected set.
					if (!value.IsNull && value.Type is BooleanType &&
					    value == true) {
						selectedSet.Add(rowIndex);
					}
				}

				result = new VirtualTable(table, selectedSet); ;
			}

			return result;
		}


		public static IEnumerable<int> SelectFromPattern(this ITable table, int column, SqlExpressionType op, Field ob) {
			if (ob.IsNull)
				return new List<int>();

			if (op == SqlExpressionType.NotLike) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.

				var likeSet = (List<int>)table.Search(column, ob.Value.ToString());
				// Don't include NULL values
				var nullCell = Field.Null(ob.Type);
				IList<int> originalSet = table.SelectRows(column, SqlExpressionType.IsNot, nullCell).ToList();
				int listSize = System.Math.Max(4, (originalSet.Count - likeSet.Count) + 4);
				List<int> resultSet = new List<int>(listSize);
				likeSet.Sort();
				int size = originalSet.Count;
				for (int i = 0; i < size; ++i) {
					int val = originalSet[i];
					// If val not in like set, add to result
					if (likeSet.BinarySearch(val) == 0) {
						resultSet.Add(val);
					}
				}
				return resultSet;
			}

			// if (op.is("like")) {
			return table.Search(column, ob.Value.ToString());
		}

		public static ITable EmptySelect(this ITable table) {
			if (table.RowCount == 0)
				return table;

			return new VirtualTable(table, new int[0]);
		}

		public static ITable DistinctBy(this ITable table, int[] columns) {
			List<int> resultList = new List<int>();
			var rowList = table.OrderRowsByColumns(columns).ToList();

			int rowCount = rowList.Count;
			int previousRow = -1;
			for (int i = 0; i < rowCount; ++i) {
				int rowIndex = rowList[i];

				if (previousRow != -1) {

					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < columns.Length && equal; ++n) {
						var c1 = table.GetValue(rowIndex, columns[n]);
						var c2 = table.GetValue(previousRow, columns[n]);
						equal = (c1.CompareTo(c2) == 0);
					}

					if (!equal) {
						resultList.Add(rowIndex);
					}
				} else {
					resultList.Add(rowIndex);
				}

				previousRow = rowIndex;
			}

			// Return the new table with distinct rows only.
			return new VirtualTable(table, resultList);
		}

		public static ITable DistinctBy(this ITable table, ObjectName[] columnNames) {
			var mapSize = columnNames.Length;
			var map = new int[mapSize];
			for (int i = 0; i < mapSize; i++) {
				map[i] = table.IndexOfColumn(columnNames[i]);
			}

			return table.DistinctBy(map);
		}

		public static ITable SelectRange(this ITable thisTable, ObjectName columnName, IndexRange[] ranges) {
			// If this table is empty then there is no range to select so
			// trivially return this object.
			if (thisTable.RowCount == 0)
				return thisTable;

			// Are we selecting a black or null range?
			if (ranges == null || ranges.Length == 0)
				// Yes, so return an empty table
				return thisTable.EmptySelect();

			// Are we selecting the entire range?
			if (ranges.Length == 1 &&
			    ranges[0].Equals(IndexRange.FullRange))
				// Yes, so return this table.
				return thisTable;

			// Must be a non-trivial range selection.

			// Find the column index of the column selected
			int column = thisTable.IndexOfColumn(columnName);

			if (column == -1) {
				throw new Exception(
					"Unable to find the column given to select the range of: " +
					columnName.Name);
			}

			// Select the range
			var rows = thisTable.SelectRowsRange(column, ranges);

			// Make a new table with the range selected
			var result = new VirtualTable(thisTable, rows.ToArray());

			// We know the new set is ordered by the column.
			result.SortColumn = column;

			return result;
		}

		#region Sub-Query

		public static bool AllRowsMatchColumnValue(this ITable table, int columnOffset, SqlExpressionType op, Field value) {
			var rows = table.SelectRows(columnOffset, op, value);
			return rows.Count() == table.RowCount;
		}

		public static bool AnyRowMatchesColumnValue(this ITable table, int columnOffset, SqlExpressionType op, Field value) {
			var rows = table.SelectRows(columnOffset, op, value);
			return rows.Count() > 0;
		}

		public static ITable SelectAnyAllNonCorrelated(this ITable table, ObjectName[] leftColumns, SqlExpressionType op, bool all, ITable rightTable) {
			if (rightTable.TableInfo.ColumnCount != leftColumns.Length) {
				throw new ArgumentException(String.Format("The right table has {0} columns that is different from the specified column names ({1})",
					rightTable.TableInfo.ColumnCount, leftColumns.Length));
			}

			// Handle trivial case of no entries to select from
			if (table.RowCount == 0)
				return table;

			// Resolve the vars in the left table and check the references are
			// compatible.
			var sz = leftColumns.Length;
			var leftColMap = new int[sz];
			var rightColMap = new int[sz];
			for (int i = 0; i < sz; ++i) {
				leftColMap[i] = table.FindColumn(leftColumns[i]);
				rightColMap[i] = i;

				if (leftColMap[i] == -1)
					throw new Exception("Invalid reference: " + leftColumns[i]);

				var leftType = table.TableInfo[leftColMap[i]].ColumnType;
				var rightType = rightTable.TableInfo[i].ColumnType;
				if (!leftType.IsComparable(rightType)) {
					throw new ArgumentException(String.Format("The type of the sub-query expression {0}({1}) " +
					                                          "is not compatible with the sub-query type {2}.",
						leftColumns[i], leftType, rightType));
				}
			}

			IEnumerable<int> rows;

			if (all) {
				// ----- ALL operation -----
				// We work out as follows:
				//   For >, >= type ALL we find the highest value in 'table' and
				//   select from 'source' all the rows that are >, >= than the
				//   highest value.
				//   For <, <= type ALL we find the lowest value in 'table' and
				//   select from 'source' all the rows that are <, <= than the
				//   lowest value.
				//   For = type ALL we see if 'table' contains a single value.  If it
				//   does we select all from 'source' that equals the value, otherwise an
				//   empty table.
				//   For <> type ALL we use the 'not in' algorithm.

				if (op == SqlExpressionType.GreaterThan || 
				    op == SqlExpressionType.GreaterOrEqualThan) {
					// Select the last from the set (the highest value),
					var highestCells = rightTable.GetLastValues(rightColMap);
					// Select from the source table all rows that are > or >= to the
					// highest cell,
					rows = table.SelectRows(leftColMap, op, highestCells);
				} else if (op == SqlExpressionType.SmallerThan || 
				           op == SqlExpressionType.SmallerOrEqualThan) {
					// Select the first from the set (the lowest value),
					var lowestCells = rightTable.GetFirstValues(rightColMap);
					// Select from the source table all rows that are < or <= to the
					// lowest cell,
					rows = table.SelectRows(leftColMap, op, lowestCells);
				} else if (op == SqlExpressionType.Equal) {
					// Select the single value from the set (if there is one).
					var singleCell = rightTable.GetSingleValues(rightColMap);
					if (singleCell != null) {
						// Select all from source_table all values that = this cell
						rows = table.SelectRows(leftColMap, op, singleCell);
					} else {
						// No single value so return empty set (no value in LHS will equal
						// a value in RHS).
						return table.EmptySelect();
					}
				} else if (op == SqlExpressionType.NotEqual) {
					// Equiv. to NOT IN
					rows = table.SelectRowsNotIn(rightTable, leftColMap, rightColMap);
				} else {
					throw new ArgumentException(String.Format("Operator of type {0} is not valid in ALL functions.", op));
				}
			} else {
				// ----- ANY operation -----
				// We work out as follows:
				//   For >, >= type ANY we find the lowest value in 'table' and
				//   select from 'source' all the rows that are >, >= than the
				//   lowest value.
				//   For <, <= type ANY we find the highest value in 'table' and
				//   select from 'source' all the rows that are <, <= than the
				//   highest value.
				//   For = type ANY we use same method from INHelper.
				//   For <> type ANY we iterate through 'source' only including those
				//   rows that a <> query on 'table' returns size() != 0.

				if (op == SqlExpressionType.GreaterThan || 
				    op == SqlExpressionType.GreaterOrEqualThan) {
					// Select the first from the set (the lowest value),
					var lowestCells = rightTable.GetFirstValues(rightColMap);
					// Select from the source table all rows that are > or >= to the
					// lowest cell,
					rows = table.SelectRows(leftColMap, op, lowestCells);
				} else if (op == SqlExpressionType.SmallerThan || 
				           op == SqlExpressionType.SmallerOrEqualThan) {
					// Select the last from the set (the highest value),
					var highestCells = rightTable.GetLastValues(rightColMap);
					// Select from the source table all rows that are < or <= to the
					// highest cell,
					rows = table.SelectRows(leftColMap, op, highestCells);
				} else if (op == SqlExpressionType.Equal) {
					// Equiv. to IN
					rows = table.SelectRowsIn(rightTable, leftColMap, rightColMap);
				} else if (op == SqlExpressionType.NotEqual) {
					// Select the value that is the same of the entire column
					var cells = rightTable.GetSingleValues(rightColMap);
					if (cells != null) {
						// All values from 'source_table' that are <> than the given cell.
						rows = table.SelectRows(leftColMap, op, cells);
					} else {
						// No, this means there are different values in the given set so the
						// query evaluates to the entire table.
						return table;
					}
				} else {
					throw new ArgumentException(String.Format("Operator of type {0} is not valid in ANY functions.", op));
				}
			}

			return new VirtualTable(table, rows.ToArray());
		}

		public static ITable Union(this ITable thisTable, ITable otherTable) {
			// Optimizations - handle trivial case of row count in one of the tables
			//   being 0.
			// NOTE: This optimization assumes this table and the unioned table are
			//   of the same type.
			if ((thisTable.RowCount == 0 && otherTable.RowCount == 0) ||
			    otherTable.RowCount == 0) {
				return thisTable;
			}

			if (thisTable.RowCount == 0)
				return otherTable;

			// First we merge this table with the input table.

			var raw1 = thisTable.GetRawTableInfo();
			var raw2 = otherTable.GetRawTableInfo();

			// This will throw an exception if the table types do not match up.

			var union = raw1.Union(raw2);

			// Now 'union' contains a list of uniquely merged rows (ie. the union).
			// Now make it into a new table and return the information.

			var tableList = union.GetTables().AsEnumerable();
			return new VirtualTable(tableList, union.GetRows());
		}

		/// <summary>
		/// This implements the <c>in</c> command.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="other"></param>
		/// <param name="column1"></param>
		/// <param name="column2"></param>
		/// <returns>
		/// Returns the rows selected from <paramref name="other"/>.
		/// </returns>
		public static IEnumerable<int> SelectRowsIn(this ITable table, ITable other, int column1, int column2) {
			// First pick the the smallest and largest table.  We only want to iterate
			// through the smallest table.
			// NOTE: This optimisation can't be performed for the 'not_in' command.

			ITable smallTable;
			ITable largeTable;
			int smallColumn;
			int largeColumn;

			if (table.RowCount < other.RowCount) {
				smallTable = table;
				largeTable = other;

				smallColumn = column1;
				largeColumn = column2;

			} else {
				smallTable = other;
				largeTable = table;

				smallColumn = column2;
				largeColumn = column1;
			}

			// Iterate through the small table's column.  If we can find identical
			// cells in the large table's column, then we should include the row in our
			// final result.

			var resultRows = new BlockIndex<int>();
			var op = SqlExpressionType.Equal;

			foreach (var row in smallTable) {
				var cell = row.GetValue(smallColumn);

				var selectedSet = largeTable.SelectRows(largeColumn, op, cell).ToList();

				// We've found cells that are IN both columns,

				if (selectedSet.Count > 0) {
					// If the large table is what our result table will be based on, append
					// the rows selected to our result set.  Otherwise add the index of
					// our small table.  This only works because we are performing an
					// EQUALS operation.

					if (largeTable == table) {
						// Only allow unique rows into the table set.
						int sz = selectedSet.Count;
						bool rs = true;
						for (int i = 0; rs && i < sz; ++i) {
							rs = resultRows.UniqueInsertSort(selectedSet[i]);
						}
					} else {
						// Don't bother adding in sorted order because it's not important.
						resultRows.Add(row.RowId.RowNumber);
					}
				}
			}

			return resultRows.ToList();
		}

		/// <summary>
		/// A multi-column version of <c>IN</c>.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="other"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IEnumerable<int> SelectRowsIn(this ITable table, ITable other, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'in' not supported yet.");

			return table.SelectRowsIn(other, t1Cols[0], t2Cols[0]);
		}

		/// <summary>
		/// This implements the <c>not in</c> command.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="other"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// <b>Issue</b>: This will be less efficient than <see cref="SelectRowsIn(ITable,ITable,int,int)">in</see> 
		/// if <paramref name="table"/> has many rows and <paramref name="other"/> has few rows.
		/// </remarks>
		/// <returns></returns>
		public static IEnumerable<int> SelectRowsNotIn(this ITable table, ITable other, int col1, int col2) {
			// Handle trivial cases
			int t2RowCount = other.RowCount;
			if (t2RowCount == 0)
				// No rows so include all rows.
				return table.SelectAllRows(col1);

			if (t2RowCount == 1) {
				// 1 row so select all from table1 that doesn't equal the value.
				var en = other.GetEnumerator();
				if (!en.MoveNext())
					throw new InvalidOperationException("Cannot iterate through table rows.");

				var cell = other.GetValue(en.Current.RowId.RowNumber, col2);
				return table.SelectRows(col1, SqlExpressionType.NotEqual, cell);
			}

			// Iterate through table1's column.  If we can find identical cell in the
			// tables's column, then we should not include the row in our final
			// result.
			List<int> resultRows = new List<int>();

			foreach (var row in table) {
				int rowIndex = row.RowId.RowNumber;
				var cell = row.GetValue(col1);

				var selectedSet = other.SelectRows(col2, SqlExpressionType.Equal, cell);

				// We've found a row in table1 that doesn't have an identical cell in
				// other, so we should include it in the result.

				if (!selectedSet.Any())
					resultRows.Add(rowIndex);
			}

			return resultRows;
		}

		/// <summary>
		/// A multi-column version of NOT IN.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="other"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IEnumerable<int> SelectRowsNotIn(this ITable table, ITable other, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'not in' not supported yet.");

			return table.SelectRowsNotIn(other, t1Cols[0], t2Cols[0]);
		}

		public static ITable NotIn(this ITable table, ITable otherTable, int[] tableColumns, int[] otherColumns) {
			return table.AsVirtual(() => SelectRowsNotIn(table, otherTable, tableColumns, otherColumns));
		}

		public static ITable Composite(this ITable table, ITable other, CompositeFunction function, bool all) {
			return new CompositeTable(table, new[] { table, other }, function, all);
		}

		public static ITable Execept(this ITable table, ITable other, bool all) {
			return table.Composite(other, CompositeFunction.Except, all);
		}

		public static ITable Intersect(this ITable table, ITable other, bool all) {
			return table.Composite(other, CompositeFunction.Intersect, all);
		}

		#endregion

		#region Join

		public static ITable Join(this ITable table, ITable otherTable, bool quick) {
			ITable outTable;

			if (quick) {
				// This implementation doesn't materialize the join
				outTable = new NaturallyJoinedTable(table, otherTable);
			} else {
				var tabs = new [] { table, otherTable};
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
					var tableSelectedSet = otherTable.Select(x => x.RowId.RowNumber).ToList();

					int tableSelectedSetSize = tableSelectedSet.Count;

					// Join with the set of rows in this table.
					var e = table.GetEnumerator();
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

		public static ITable NaturalJoin(this ITable table, ITable otherTable) {
			return table.Join(otherTable, true);
		}

		public static ITable Join(this ITable table, IRequest context, ITable other, ObjectName columnName, SqlExpressionType operatorType,
			SqlExpression expression) {
			var rightExpression = expression;
			// If the rightExpression is a simple variable then we have the option
			// of optimizing this join by putting the smallest table on the LHS.
			var rhsVar = rightExpression.AsReferenceName();
			var lhsVar = columnName;
			var op = operatorType;

			if (rhsVar != null) {
				// We should arrange the expression so the right table is the smallest
				// of the sides.
				// If the left result is less than the right result

				if (table.RowCount < other.RowCount) {
					// Reverse the join
					rightExpression = SqlExpression.Reference(lhsVar);
					lhsVar = rhsVar;
					op = op.Reverse();

					// Reverse the tables.
					var t = other;
					other = table;
					table = t;
				}
			}

			var joinExp = SqlExpression.Binary(SqlExpression.Reference(lhsVar), op, rightExpression);

			// The join operation.
			return table.SimpleJoin(context, other, joinExp);
		}

		public static ITable SimpleJoin(this ITable thisTable, IRequest context, ITable other, SqlBinaryExpression binary) {
			var objRef = binary.Left as SqlReferenceExpression;
			if (objRef == null)
				throw new ArgumentException();

			// Find the row with the name given in the condition.
			int lhsColumn = thisTable.FindColumn(objRef.ReferenceName);

			if (lhsColumn == -1)
				throw new Exception("Unable to find the LHS column specified in the condition: " + objRef.ReferenceName);

			// Create a variable resolver that can resolve columns in the destination
			// table.
			var resolver = other.GetVariableResolver();

			// The join algorithm.  It steps through the RHS expression, selecting the
			// cells that match the relation from the LHS table (this table).

			var thisRowSet = new List<int>();
			var tableRowSet = new List<int>();

			var e = other.GetEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.Current.RowId.RowNumber;

				var rowResolver = resolver.ForRow(rowIndex);

				// Select all the rows in this table that match the joining condition.
				var selectedSet = thisTable.SelectRows(rowResolver, context, binary);

				var selectList = selectedSet.ToList();

				var size = selectList.Count;
				// Include in the set.
				for (int i = 0; i < size; i++) {
					tableRowSet.Add(rowIndex);
				}

				thisRowSet.AddRange(selectList);
			}

			// Create the new VirtualTable with the joined tables.

			var tabs = new[] {thisTable, other};
			var rowSets = new IList<int>[] {thisRowSet, tableRowSet};

			return new VirtualTable(tabs, rowSets);
		}

		public static ITable OuterJoin(this ITable table, ITable rightTable) {
			// Form the row list for right hand table,
			var rowList = rightTable.Select(x => x.RowId.RowNumber).ToList();

			int colIndex = rightTable.IndexOfColumn(table.GetResolvedColumnName(0));
			rowList = rightTable.ResolveRows(colIndex, rowList, table).ToList();

			// This row set
			var thisTableSet = table.Select(x => x.RowId.RowNumber).ToList();

			thisTableSet.Sort();
			rowList.Sort();

			// Find all rows that are in 'this table' and not in 'right'
			List<int> resultList = new List<int>(96);
			int size = thisTableSet.Count;
			int rowListIndex = 0;
			int rowListSize = rowList.Count;
			for (int i = 0; i < size; ++i) {
				int thisVal = thisTableSet[i];
				if (rowListIndex < rowListSize) {
					int inVal = rowList[rowListIndex];
					if (thisVal < inVal) {
						resultList.Add(thisVal);
					} else if (thisVal == inVal) {
						while (rowListIndex < rowListSize &&
						       rowList[rowListIndex] == inVal) {
							++rowListIndex;
						}
					} else {
						throw new InvalidOperationException("'this_val' > 'in_val'");
					}
				} else {
					resultList.Add(thisVal);
				}
			}

			// Return the new VirtualTable
			return new VirtualTable(table, resultList);
		}

		//public static ITable EquiJoin(this ITable table, IRequest context, ITable other, ObjectName[] leftColumns, ObjectName[] rightColumns) {
		//	// TODO: This needs to migrate to a better implementation that
		//	//   exploits multi-column indexes if one is defined that can be used.

		//	var firstLeft = SqlExpression.Reference(leftColumns[0]);
		//	var firstRight = SqlExpression.Reference(rightColumns[0]);
		//	var onExpression = SqlExpression.Equal(firstLeft, firstRight);

		//	var result = table.SimpleJoin(context, other, onExpression);

		//	int sz = leftColumns.Length;

		//	// If there are columns left to equi-join, we resolve the rest with a
		//	// single exhaustive select of the form,
		//	//   ( table1.col2 = table2.col2 AND table1.col3 = table2.col3 AND ... )
		//	if (sz > 1) {
		//		// Form the expression
		//		SqlExpression restExpression = null;
		//		for (int i = 1; i < sz; ++i) {
		//			var left = SqlExpression.Reference(leftColumns[i]);
		//			var right = SqlExpression.Reference(rightColumns[i]);
		//			var equalExp = SqlExpression.And(left, right);

		//			if (restExpression == null) {
		//				restExpression = equalExp;
		//			} else {
		//				restExpression = SqlExpression.And(restExpression, equalExp);
		//			}
		//		}

		//		result = result.ExhaustiveSelect(context, restExpression);
		//	}

		//	return result;
		//}

		#endregion

		#region Order By

		public static IEnumerable<int> OrderRowsByColumns(this ITable table, int[] columns) {
			var work = table.OrderBy(columns);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			var rowList = work.Select(row => row.RowId.RowNumber);

			return work.ResolveRows(0, rowList, table);
		}


		public static ITable OrderBy(this ITable table, int[] columns) {
			// Sort by the column list.
			ITable resultTable = table;
			for (int i = columns.Length - 1; i >= 0; --i) {
				resultTable = resultTable.OrderBy(columns[i], true);
			}

			// A nice post condition to check on.
			if (resultTable.RowCount != table.RowCount)
				throw new InvalidOperationException("The final row count mismatches.");

			return resultTable;
		}

		public static ITable OrderBy(this ITable table, int columnIndex, bool ascending) {
			if (table == null)
				return null;

			var rows = table.SelectAllRows(columnIndex);

			// Reverse the list if we are not ascending
			if (@ascending == false)
				rows = rows.Reverse();

			return new VirtualTable(table, rows.ToArray());
		}

		public static ITable OrderBy(this ITable table, ObjectName columnName, bool ascending) {
			var columnOffset = table.IndexOfColumn(columnName);
			if (columnOffset == -1)
				throw new ArgumentException(String.Format("Column '{0}' was not found in table.", columnName));

			return table.OrderBy(columnOffset, @ascending);
		}

		public static ITable OrderBy(this ITable table, ObjectName[] columnNames, bool[] ascending) {
			var result = table;
			// Sort the results by the columns in reverse-safe order.
			int sz = ascending.Length;
			for (int n = sz - 1; n >= 0; --n) {
				result = result.OrderBy(columnNames[n], ascending[n]);
			}
			return result;
		}

		public static ITable OrderBy(this ITable table, string columnName, bool ascending) {
			return table.OrderBy(table.ResolveColumnName(columnName), ascending);
		}

		#endregion

		public static ITable Subset(this ITable table, ObjectName[] columnNames, ObjectName[] aliases) {
			var columnMap = new int[columnNames.Length];

			for (int i = 0; i < columnMap.Length; i++) {
				columnMap[i] = table.IndexOfColumn(columnNames[i]);
			}

			return new SubsetColumnTable(table, columnMap, aliases);
		}

		#endregion

		public static bool Exists(this ITable table, int columnOffset, Field value) {
			return table.SelectRowsEqual(columnOffset, value).Any();
		}

		public static bool Exists(this ITable table, int columnOffset1, Field value1, int columnOffset2, Field value2) {
			return table.SelectRowsEqual(columnOffset1, value1, columnOffset2, value2).Any();
		}

		private static ITable AsVirtual(this ITable table, Func<IEnumerable<int>> selector) {
			return new VirtualTable(table, selector().ToArray());
		}

		//public static ITable ColumnMerge(this ITable table, ITable other) {
		//	if (table.RowCount != other.RowCount)
		//		throw new InvalidOperationException("Tables have different row counts.");

		//	// Create the new VirtualTable with the joined tables.

		//	List<int> allRowSet = new List<int>();
		//	int rcount = table.RowCount;
		//	for (int i = 0; i < rcount; ++i) {
		//		allRowSet.Add(i);
		//	}

		//	var tabs = new[] { table, other };
		//	var rowSets = new IList<int>[] { allRowSet, allRowSet };

		//	return new VirtualTable(tabs, rowSets);
		//}

		public static Dictionary<string, Objects.ISqlObject> ToDictionary(this ITable table) {
			if (table.TableInfo.ColumnCount != 2)
				throw new NotSupportedException("Table must have two columns.");

			var map = new Dictionary<string, Objects.ISqlObject>();
			foreach (var row in table) {
				var key = row.GetValue(0);
				var value = row.GetValue(1);
				map[key.AsVarChar().Value.ToString()] = value.Value;
			}

			return map;
		}
	}
}