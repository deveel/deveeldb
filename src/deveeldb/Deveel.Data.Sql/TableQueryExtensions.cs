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
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides a set of extension methods to <see cref="ITable"/>
	/// and <see cref="IMutableTable"/> objects.
	/// </summary>
	public static class TableQueryExtensions {
		#region Get Value

		public static DataObject GetLastValue(this ITable table, int columnOffset) {
			var rows = table.SelectLast(columnOffset).ToList();
			return rows.Count > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static DataObject[] GetLastValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetLastValue(columnOffsets[0])};
		}

		public static DataObject GetFirstValue(this ITable table, int columnOffset) {
			var rows = table.SelectFirst(columnOffset).ToList();
			return rows.Count > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static DataObject[] GetFirstValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetFirstValue(columnOffsets[0])};
		}

		public static DataObject GetSingleValue(this ITable table, int columnOffset) {
			IList<int> rows = table.SelectFirst(columnOffset).ToList();
			int sz = rows.Count;
			return sz == table.RowCount && sz > 0 ? table.GetValue(rows[0], columnOffset) : null;
		}

		public static DataObject[] GetSingleValues(this ITable table, int[] columnOffsets) {
			if (columnOffsets.Length > 1)
				throw new ArgumentException("Multi-column gets not supported.");

			return new[] {table.GetSingleValue(columnOffsets[0])};
		}

		#endregion

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

		public static IEnumerable<int> SelectLast(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectLast();
		}

		public static IEnumerable<int> SelectFirst(this ITable table, int columnOffset) {
			return table.GetIndex(columnOffset).SelectFirst();
		}

		public static bool Exists(this ITable table, int columnOffset, DataObject value) {
			return table.SelectEqual(columnOffset, value).Any();
		}

		public static bool Exists(this ITable table, int columnOffset1, DataObject value1, int columnOffset2, DataObject value2) {
			return table.SelectEqual(columnOffset1, value1, columnOffset2, value2).Any();
		}

		public static IEnumerable<int> SelectRows(this ITable table, int[] columnOffsets, BinaryOperator op,
			DataObject[] values) {
			if (columnOffsets.Length > 1)
				throw new NotSupportedException("Multi-column selects not supported yet.");

			return table.SelectRows(columnOffsets[0], op, values[0]);
		}

		public static IEnumerable<int> SelectBetween(this ITable table, int column, DataObject minCell, DataObject maxCell) {
			// Check all the tables are comparable
			var colType = table.TableInfo[column].ColumnType;
			if (!minCell.Type.IsComparable(colType) ||
				!maxCell.Type.IsComparable(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			return table.GetIndex(column).SelectBetween(minCell, maxCell);
		}

		public static IEnumerable<int> SelectRows(this ITable table, int column, BinaryOperator op, DataObject value) {
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
			if (op.OperatorType == BinaryOperatorType.Equal)
				return index.SelectEqual(value);
			if (op.OperatorType == BinaryOperatorType.NotEqual)
				return index.SelectNotEqual(value);
			if (op.OperatorType == BinaryOperatorType.GreaterThan)
				return index.SelectGreater(value);
			if (op.OperatorType == BinaryOperatorType.SmallerThan)
				return index.SelectLess(value);
			if (op.OperatorType == BinaryOperatorType.GreaterOrEqualThan)
				return index.SelectGreaterOrEqual(value);
			if (op.OperatorType == BinaryOperatorType.SmallerOrEqualThan)
				return index.SelectLessOrEqual(value);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			var rangeSet = new IndexRangeSet();
			rangeSet = rangeSet.Intersect(op, value);
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

		public static ITable SimpleSelect(this ITable table, IQueryContext context, ObjectName columnName, BinaryOperator op, SqlExpression exp) {
			var dbTable = table as IDbTable;
			if (dbTable == null)
				throw new NotSupportedException();

			// Find the row with the name given in the condition.
			int column = dbTable.FindColumn(columnName);

			if (column == -1)
				throw new ArgumentException(String.Format("Unable to find the column {0} in the condition.", columnName.Name));

			// If we are doing a sub-query search
			if (op.IsSubQuery) {

				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				if (exp.ExpressionType != SqlExpressionType.Constant &&
				    exp.ExpressionType != SqlExpressionType.Tuple)
					throw new ArgumentException();

				IEnumerable<SqlExpression> list;

				if (exp.ExpressionType == SqlExpressionType.Constant) {
					var tob = ((SqlConstantExpression) exp).Value;
					if (tob.Type is ArrayType) {
						var array = (SqlArray) tob.Value;
						list = array;
					} else {
						throw new Exception("Error with format or RHS expression.");
					}
				} else {
					list = ((SqlTupleExpression) exp).Expressions;
				}

				// Construct a temporary table with a single column that we are
				// comparing to.
				var col = table.TableInfo[column];
				var ttable = TemporaryTable.SingleColumnTable(dbTable.Database, col.ColumnName, col.ColumnType);

				foreach (var expression in list) {
					var rowNum = ttable.NewRow();

					var evalExp = (SqlConstantExpression)expression.Evaluate(context, null, null);
					ttable.SetValue(rowNum, 0, evalExp.Value);
				}

				ttable.BuildIndexes();

				// Perform the any/all sub-query on the constant table.

				return table.AnyAllNonCorrelated(new[] { columnName }, op, ttable);
			}

			{
				if (!exp.IsConstant())
					throw new ArgumentException();

				var evalExp = exp.Evaluate(context, null);
				if (evalExp.ExpressionType != SqlExpressionType.Constant)
					throw new InvalidOperationException();

				var value = ((SqlConstantExpression) evalExp).Value;

				IEnumerable<int> rows;

				if (op.IsOfType(BinaryOperatorType.Like) ||
				    op.IsOfType(BinaryOperatorType.NotLike)
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

				return new VirtualTable(dbTable, rows) {SortColumn = column};
			}
		}

		public static IEnumerable<int> SelectFromPattern(this ITable table, int column, BinaryOperator op, DataObject ob) {
			if (ob.IsNull)
				return new List<int>();

			if (op.IsOfType(BinaryOperatorType.NotLike)) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.

				var likeSet = (List<int>)table.Search(column, ob.ToString());
				// Don't include NULL values
				var nullCell = DataObject.Null(ob.Type);
				IList<int> originalSet = table.SelectRows(column, BinaryOperator.IsNot, nullCell).ToList();
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
			return table.Search(column, ob.ToString());
		}

		/// <summary>
		/// This is the search method.</summary>
		/// <remarks>
		/// It requires a table to search, a column of the table, and a pattern.
		/// It returns the rows in the table that match the pattern if any. 
		/// Pattern searching only works successfully on columns that are of 
		/// type <see cref="DbType.String"/>. This works by first reducing the 
		/// search to all cells that contain the first section of text. ie. 
		/// <c>pattern = "Anto% ___ano"</c> will first reduce search to all 
		/// rows between <i>Anto</i> and <i>Anton</i>. This makes for better
		/// efficiency.
		/// </remarks>
		public static IEnumerable<int> Search(this ITable table, int column, string pattern) {
			return table.Search(column, pattern, '\\');
		}

		public static IEnumerable<int> Search(this ITable table, int column, String pattern, char escapeChar) {
			var colType = table.TableInfo[column].ColumnType;

			// If the column type is not a string type then report an error.
			if (!(colType is StringType))
				throw new ApplicationException("Unable to perform a pattern search on a non-String type column.");

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

				var cell = new DataObject(colType, new SqlString(pattern));
				return table.SelectRows(column, BinaryOperator.Equal, cell);
			}

			if (prePattern.Length == 0 ||
				colStringType.Locale != null) {

				// No pre-pattern easy search :-(.  This is either because there is no
				// pre pattern (it starts with a wild-card) or the locale of the string
				// is non-lexicographical.  In either case, we need to select all from
				// the column and brute force the search space.

				searchCase = table.SelectAll(column);
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

				var cellLower = new DataObject(colType, new SqlString(lowerBounds));
				var cellUpper = new DataObject(colType, new SqlString(upperBounds));

				// Select rows between these two points.

				searchCase = table.SelectBetween(column, cellLower, cellUpper);
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

		public static ITable EmptySelect(this ITable table) {
			if (table.RowCount == 0)
				return table;

			var dbTable = table as IDbTable;
			if (dbTable == null)
				throw new NotSupportedException();

			return new VirtualTable(dbTable, new int[0]);
		}

		#region Sub-Query

		public static ITable AnyAllNonCorrelated(this ITable table, ObjectName[] leftColumns, BinaryOperator op, ITable rightTable) {
			if (rightTable.TableInfo.ColumnCount != leftColumns.Length) {
				throw new ArgumentException(String.Format("The right table has {0} columns that is different from the specified column names ({1})",
						rightTable.TableInfo.ColumnCount, leftColumns.Length));
			}

			// Handle trivial case of no entries to select from
			if (table.RowCount == 0)
				return table;

			var dbTable = table as IDbTable;
			if (dbTable == null)
				throw new NotSupportedException();

			// Resolve the vars in the left table and check the references are
			// compatible.
			var sz = leftColumns.Length;
			var leftColMap = new int[sz];
			var rightColMap = new int[sz];
			for (int i = 0; i < sz; ++i) {
				leftColMap[i] = dbTable.FindColumn(leftColumns[i]);
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

			if (!op.IsSubQuery)
				throw new ArgumentException(String.Format("The operator {0} is not a sub-query form.", op));

			if (op.SubQueryType == OperatorSubType.All) {
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

				if (op.IsOfType(BinaryOperatorType.GreaterThan) || 
					op.IsOfType(BinaryOperatorType.GreaterOrEqualThan)) {
					// Select the last from the set (the highest value),
					var highestCells = rightTable.GetLastValues(rightColMap);
					// Select from the source table all rows that are > or >= to the
					// highest cell,
					rows = table.SelectRows(leftColMap, op, highestCells);
				} else if (op.IsOfType(BinaryOperatorType.SmallerThan) || 
					op.IsOfType(BinaryOperatorType.SmallerOrEqualThan)) {
					// Select the first from the set (the lowest value),
					var lowestCells = rightTable.GetFirstValues(rightColMap);
					// Select from the source table all rows that are < or <= to the
					// lowest cell,
					rows = table.SelectRows(leftColMap, op, lowestCells);
				} else if (op.IsOfType(BinaryOperatorType.Equal)) {
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
				} else if (op.IsOfType(BinaryOperatorType.NotEqual)) {
					// Equiv. to NOT IN
					rows = table.NotIn(rightTable, leftColMap, rightColMap);
				} else {
					throw new ArgumentException(String.Format("Operator of type {0} is not valid in ALL functions.", op.OperatorType));
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

				if (op.IsOfType(BinaryOperatorType.GreaterThan) || 
					op.IsOfType(BinaryOperatorType.GreaterOrEqualThan)) {
					// Select the first from the set (the lowest value),
					var lowestCells = rightTable.GetFirstValues(rightColMap);
					// Select from the source table all rows that are > or >= to the
					// lowest cell,
					rows = table.SelectRows(leftColMap, op, lowestCells);
				} else if (op.IsOfType(BinaryOperatorType.SmallerThan) || 
					op.IsOfType(BinaryOperatorType.SmallerOrEqualThan)) {
					// Select the last from the set (the highest value),
					var highestCells = rightTable.GetLastValues(rightColMap);
					// Select from the source table all rows that are < or <= to the
					// highest cell,
					rows = table.SelectRows(leftColMap, op, highestCells);
				} else if (op.IsOfType(BinaryOperatorType.Equal)) {
					// Equiv. to IN
					rows = table.In(rightTable, leftColMap, rightColMap);
				} else if (op.IsOfType(BinaryOperatorType.NotEqual)) {
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
					throw new ArgumentException(String.Format("Operator of type {0} is not valid in ANY functions.", op.OperatorType));
				}
			}

			return new VirtualTable(dbTable, rows);
		}

		/// <summary>
		/// This implements the <c>in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="column1"></param>
		/// <param name="column2"></param>
		/// <returns>
		/// Returns the rows selected from <paramref name="table1"/>.
		/// </returns>
		public static IEnumerable<int> In(this ITable table, ITable table2, int column1, int column2) {
			// First pick the the smallest and largest table.  We only want to iterate
			// through the smallest table.
			// NOTE: This optimisation can't be performed for the 'not_in' command.

			ITable smallTable;
			ITable largeTable;
			int smallColumn;
			int largeColumn;

			if (table.RowCount < table2.RowCount) {
				smallTable = table;
				largeTable = table2;

				smallColumn = column1;
				largeColumn = column2;

			} else {
				smallTable = table2;
				largeTable = table;

				smallColumn = column2;
				largeColumn = column1;
			}

			// Iterate through the small table's column.  If we can find identical
			// cells in the large table's column, then we should include the row in our
			// final result.

			var resultRows = new BlockIndex<int>();
			var op = BinaryOperator.Equal;

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
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IEnumerable<int> In(this ITable table, ITable table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'in' not supported yet.");

			return table.In(table2, t1Cols[0], t2Cols[0]);
		}

		/// <summary>
		/// This implements the <c>not in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// <b>Issue</b>: This will be less efficient than <see cref="In(Table,Table,int,int)">in</see> 
		/// if <paramref name="table1"/> has many rows and <paramref name="table2"/> has few rows.
		/// </remarks>
		/// <returns></returns>
		public static IEnumerable<int> NotIn(this ITable table, ITable table2, int col1, int col2) {
			// Handle trivial cases
			int t2RowCount = table2.RowCount;
			if (t2RowCount == 0)
				// No rows so include all rows.
				return table.SelectAll(col1);

			if (t2RowCount == 1) {
				// 1 row so select all from table1 that doesn't equal the value.
				var en = table2.GetEnumerator();
				if (!en.MoveNext())
					throw new InvalidOperationException("Cannot iterate through table rows.");

				var cell = table2.GetValue(en.Current.RowId.RowNumber, col2);
				return table.SelectRows(col1, BinaryOperator.NotEqual, cell);
			}

			// Iterate through table1's column.  If we can find identical cell in the
			// tables's column, then we should not include the row in our final
			// result.
			List<int> resultRows = new List<int>();

			foreach (var row in table) {
				int rowIndex = row.RowId.RowNumber;
				var cell = row.GetValue(col1);

				var selectedSet = table2.SelectRows(col2, BinaryOperator.Equal, cell);

				// We've found a row in table1 that doesn't have an identical cell in
				// table2, so we should include it in the result.

				if (!selectedSet.Any())
					resultRows.Add(rowIndex);
			}

			return resultRows;
		}

		/// <summary>
		/// A multi-column version of NOT IN.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IEnumerable<int> NotIn(this ITable table, ITable table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'not in' not supported yet.");

			return table.NotIn(table2, t1Cols[0], t2Cols[0]);
		}

		#endregion

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