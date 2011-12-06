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

using Deveel.Data.Collections;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data {
	public abstract partial class Table {
		/// <summary>
		/// Select all the rows of the table matching the given values for the
		/// given columns.
		/// </summary>
		/// <param name="columnIndexes"></param>
		/// <param name="op"></param>
		/// <param name="cellValues"></param>
		/// <remarks>
		/// Multi-select columns not yet supported.
		/// <para>
		/// <b>Note:</b> This can be used to exploit multi-column indexes 
		/// if they exist.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a set that respresents the list of multi-column row numbers
		/// selected from the table given the condition.
		/// </returns>
		internal IntegerVector SelectRows(int[] columnIndexes, Operator op, TObject[] cellValues) {
			// PENDING: Look for an multi-column index to make this a lot faster,
			if (columnIndexes.Length > 1)
				throw new ApplicationException("Multi-column select not supported.");

			return SelectRows(columnIndexes[0], op, cellValues[0]);
		}

		/// <summary>
		/// Select all the rows of the table matching the given value for the
		/// given column.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <param name="op"></param>
		/// <param name="cellValue"></param>
		/// <returns>
		/// Returns a set that respresents the list of row numbers
		/// selected from the table given the condition.
		/// </returns>
		internal IntegerVector SelectRows(int columnIndex, Operator op, TObject cellValue) {
			// If the cell is of an incompatible type, return no results,
			TType colType = GetTTypeForColumn(columnIndex);
			if (!cellValue.TType.IsComparableType(colType))
				// Types not comparable, so return 0
				return new IntegerVector(0);

			// Get the selectable scheme for this column
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);

			// If the operator is a standard operator, use the interned SelectableScheme
			// methods.
			if (op.IsEquivalent("="))
				return ss.SelectEqual(cellValue);
			if (op.IsEquivalent("<>"))
				return ss.SelectNotEqual(cellValue);
			if (op.IsEquivalent(">"))
				return ss.SelectGreater(cellValue);
			if (op.IsEquivalent("<"))
				return ss.SelectLess(cellValue);
			if (op.IsEquivalent(">="))
				return ss.SelectGreaterOrEqual(cellValue);
			if (op.IsEquivalent("<="))
				return ss.SelectLessOrEqual(cellValue);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			SelectableRangeSet rangeSet = new SelectableRangeSet();
			rangeSet.Intersect(op, cellValue);
			return ss.SelectRange(rangeSet.ToArray());
		}

		/// <summary>
		/// Selects the rows in a table column between two minimum and maximum 
		/// bounds.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <param name="minCellValue"></param>
		/// <param name="maxCellValue"></param>
		/// <remarks>
		/// <b>Note</b> The returns IntegerList <b>must</b> be sorted be the 
		/// <paramref name="columnIndex"/> cells.
		/// </remarks>
		/// <returns>
		/// Returns all the rows in the table with the value of <paramref name="columnIndex"/>
		/// column greater or equal then <paramref name="minCellValue"/> and smaller then
		/// <paramref name="maxCellValue"/>.
		/// </returns>
		internal virtual IntegerVector SelectRows(int columnIndex, TObject minCellValue, TObject maxCellValue) {
			// Check all the tables are comparable
			TType colType = GetTTypeForColumn(columnIndex);
			if (!minCellValue.TType.IsComparableType(colType) ||
			    !maxCellValue.TType.IsComparableType(colType))
				// Types not comparable, so return 0
				return new IntegerVector(0);

			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectBetween(minCellValue, maxCellValue);
		}

		/// <summary>
		/// Selects all the rows where the given column matches the regular
		/// expression.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <param name="op"></param>
		/// <param name="cellValue"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to 
		/// perform the operation.
		/// <para>
		/// This method must guarentee the result is ordered by the given 
		/// column index.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IntegerVector SelectFromRegex(int columnIndex, Operator op, TObject cellValue) {
			if (cellValue.IsNull)
				return new IntegerVector(0);

			return PatternSearch.RegexSearch(this, columnIndex, cellValue.Object.ToString());
		}

		/// <summary>
		/// Selects all the rows where the given column matches the 
		/// given pattern.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <param name="op">Operator for the selection (either <c>LIKE</c> 
		/// or <c>NOT LIKE</c>).</param>
		/// <param name="cellValue"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to perform 
		/// these operations.
		/// </remarks>
		/// <returns></returns>
		internal IntegerVector SelectFromPattern(int columnIndex, Operator op, TObject cellValue) {
			if (cellValue.IsNull)
				return new IntegerVector();

			if (op.IsEquivalent("not like")) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.
				IntegerVector likeSet = PatternSearch.Search(this, columnIndex, cellValue.ToString());
				// Don't include NULL values
				TObject nullCell = new TObject(cellValue.TType, null);
				IntegerVector originalSet = SelectRows(columnIndex, Operator.IsNot, nullCell);
				int vecSize = SysMath.Max(4, (originalSet.Count - likeSet.Count) + 4);
				IntegerVector resultSet = new IntegerVector(vecSize);
				likeSet.QuickSort();
				int size = originalSet.Count;
				for (int i = 0; i < size; ++i) {
					int val = originalSet[i];
					// If val not in like set, add to result
					if (likeSet.SortedIntCount(val) == 0)
						resultSet.AddInt(val);
				}
				return resultSet;
			}

			// if (op.is("like")) {
			return PatternSearch.Search(this, columnIndex, cellValue.ToString());
		}

		/// <summary>
		/// Returns a list of rows that represents the enumerator order of 
		/// this table.
		/// </summary>
		/// <returns></returns>
		public IntegerVector SelectAll() {
			IntegerVector list = new IntegerVector(RowCount);
			IRowEnumerator en = GetRowEnumerator();
			while (en.MoveNext()) {
				list.AddInt(en.RowIndex);
			}
			return list;
		}

		/// <summary>
		/// Returns a list that represents the sorted order of this table of all
		/// values in the given SelectableRange objects of the given column index.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <param name="ranges"></param>
		/// <remarks>
		/// If there is an index on the column, the result can be found very quickly.
		/// The range array must be normalized (no overlapping ranges).
		/// </remarks>
		/// <returns></returns>
		public IntegerVector SelectRange(int columnIndex, SelectableRange[] ranges) {
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectRange(ranges);
		}

		/// <summary>
		/// Returns a list that represents the last sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public IntegerVector SelectLast(int columnIndex) {
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectLast();
		}

		/// <summary>
		/// Returns a list that represents the first sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public IntegerVector SelectFirst(int columnIndex) {
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectFirst();
		}

		/// <summary>
		/// Returns a list that represents the rest of the sorted element(s) of the
		/// given column index (not the <i>first</i> set).
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public IntegerVector SelectRest(int columnIndex) {
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectNotFirst();
		}

		/// <summary>
		/// Returns a new table with any duplicate rows in 
		/// this table removed.
		/// </summary>
		/// <returns></returns>
		[Obsolete("Deprecated: not a proper SQL DISTINCT", false)]
		public VirtualTable Distinct() {
			RawTableInformation raw = ResolveToRawTable(new RawTableInformation());
			raw.removeDuplicates();

			Table[] tableList = raw.GetTables();
			VirtualTable tableOut = new VirtualTable(tableList);
			tableOut.Set(tableList, raw.GetRows());

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, tableOut + " = " + this + ".Distinct()");
#endif

			return tableOut;
		}

		/// <summary>
		/// Returns a new table that has only distinct rows in it.
		/// </summary>
		/// <param name="columnIndexes">Integer array containing the columns 
		/// to make distinct over.</param>
		/// <remarks>
		/// This is an expensive operation. We sort over all the columns, then 
		/// iterate through the result taking out any duplicate rows.
		/// <para>
		/// <b>Note</b>: This will change the order of this table in the result.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table Distinct(int[] columnIndexes) {
			IntegerVector resultList = new IntegerVector();
			IntegerVector rowList = OrderedRowList(columnIndexes);

			int rowCount = rowList.Count;
			int previousRow = -1;
			for (int i = 0; i < rowCount; ++i) {
				int rowIndex = rowList[i];

				if (previousRow != -1) {
					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < columnIndexes.Length && equal; ++n) {
						TObject c1 = GetCellContents(columnIndexes[n], rowIndex);
						TObject c2 = GetCellContents(columnIndexes[n], previousRow);
						equal = (c1.CompareTo(c2) == 0);
					}

					if (!equal)
						resultList.AddInt(rowIndex);
				} else {
					resultList.AddInt(rowIndex);
				}

				previousRow = rowIndex;
			}

			// Return the new table with distinct rows only.
			VirtualTable vt = new VirtualTable(this);
			vt.Set(this, resultList);

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, vt + " = " + this + ".Distinct(" + columnIndexes + ")");
#endif

			return vt;
		}

		/// <summary>
		/// Returns a new table based on this table with no rows in it.
		/// </summary>
		/// <returns></returns>
		public Table EmptySelect() {
			if (RowCount == 0)
				return this;

			VirtualTable table = new VirtualTable(this);
			table.Set(this, new IntegerVector(0));
			return table;
		}

		/// <summary>
		/// Selects a single row at the given index from this table.
		/// </summary>
		/// <param name="row_index"></param>
		/// <returns></returns>
		public Table SingleRowSelect(int row_index) {
			VirtualTable table = new VirtualTable(this);
			IntegerVector ivec = new IntegerVector(1);
			ivec.AddInt(row_index);
			table.Set(this, ivec);
			return table;
		}

		/// <summary>
		/// A single column range select on this table.
		/// </summary>
		/// <param name="columnName">The column variable in this table (eg. Part.id)</param>
		/// <param name="ranges">The normalized (no overlapping) set of ranges to find.</param>
		/// <remarks>
		/// This can often be solved very quickly especially if there is an index 
		/// on the column.  The <see cref="SelectableRange"/> array represents a 
		/// set of ranges that are returned that meet the given criteria.
		/// </remarks>
		/// <returns></returns>
		public Table RangeSelect(VariableName columnName, SelectableRange[] ranges) {
			// If this table is empty then there is no range to select so
			// trivially return this object.
			if (RowCount == 0)
				return this;

			// Are we selecting a black or null range?
			if (ranges == null || ranges.Length == 0)
				// Yes, so return an empty table
				return EmptySelect();

			// Are we selecting the entire range?
			if (ranges.Length == 1 &&
			    ranges[0].Equals(SelectableRange.FULL_RANGE))
				// Yes, so return this table.
				return this;

			// Must be a non-trivial range selection.

			// Find the column index of the column selected
			int column = FindFieldName(columnName);

			if (column == -1)
				throw new Exception("Unable to find the column given to select the range of: " + columnName.Name);

			// Select the range
			IntegerVector rows = SelectRange(column, ranges);

			// Make a new table with the range selected
			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// We know the new set is ordered by the column.
			table.OptimisedPostSet(column);

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, table + " = " + this + ".RangeSelect(" + columnName + ", " + ranges + " )");
#endif

			return table;
		}

		/// <summary>
		/// A simple select on this table.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="columnName">The left has side column reference.</param>
		/// <param name="op">The operator.</param>
		/// <param name="expression">The expression to select against (the 
		/// expression <b>must</b> be a constant).</param>
		/// <remarks>
		/// We select against a column, with an <see cref="Operator"/> and a 
		/// rhs <see cref="Expression"/> that is constant (only needs to be 
		/// evaluated once).
		/// </remarks>
		/// <returns></returns>
		public Table SimpleSelect(IQueryContext context, VariableName columnName, Operator op, Expression expression) {
#if DEBUG
			string debugSelectWith;
#endif

			// Find the row with the name given in the condition.
			int column = FindFieldName(columnName);

			if (column == -1)
				throw new Exception("Unable to find the LHS column specified in the condition: " + columnName.Name);

			IntegerVector rows;

			bool orderedBySelectColumn;

			// If we are doing a sub-query search
			if (op.IsSubQuery) {
				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				object ob = expression.Last;
				if (!(ob is TObject))
					throw new Exception("Sub-query not a TObject");

				TObject tob = (TObject)ob;
				if (tob.TType is TArrayType) {
					Expression[] list = (Expression[])tob.Object;

					// Construct a temporary table with a single column that we are
					// comparing to.
					DataTableColumnInfo columnInfo = GetColumn(FindFieldName(columnName));
					DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
					TemporaryTable ttable = new TemporaryTable(dbContext.Database, "single", new DataTableColumnInfo[] { columnInfo });

					for (int i = 0; i < list.Length; ++i) {
						ttable.NewRow();
						ttable.SetRowObject(list[i].Evaluate(null, null, context), 0);
					}

					ttable.SetupAllSelectableSchemes();

					// Perform the any/all sub-query on the constant table.

					return TableFunctions.AnyAllNonCorrelated(this, new VariableName[] { columnName }, op, ttable);
				}

				throw new Exception("Error with format or RHS expression.");
			}
				
			// If we are doing a LIKE or REGEX pattern search
			if (op.IsEquivalent("like") || op.IsEquivalent("not like") || op.IsEquivalent("regex")) {
				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject rhsConst = expression.Evaluate(null, context);

				if (op.IsEquivalent("regex")) {
					// Use the regular expression search to determine matching rows.
					rows = SelectFromRegex(column, op, rhsConst);
				} else {
					// Use the standard SQL pattern matching engine to determine
					// matching rows.
					rows = SelectFromPattern(column, op, rhsConst);
				}
				// These searches guarentee result is ordered by the column
				orderedBySelectColumn = true;

				// Describe the 'LIKE' select
#if DEBUG
				debugSelectWith = op + " " + rhsConst;
#endif

			} else {
				// Otherwise, we doing an index based comparison.

				// Is the column we are searching on indexable?
				DataTableColumnInfo colInfo = GetColumn(column);
				if (!colInfo.IsIndexableType)
					throw new StatementException("Can not search on field type " + colInfo.SQLTypeString + " in '" + colInfo.Name + "'");

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject rhsConst = expression.Evaluate(null, context);

				// Get the rows of the selected set that match the given condition.
				rows = SelectRows(column, op, rhsConst);
				orderedBySelectColumn = true;

				// Describe the select
#if DEBUG
				debugSelectWith = op + " " + rhsConst;
#endif
			}

			// We now has a set of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// OPTIMIZATION: Since we know that the 'select' return is ordered by the
			//   LHS column, we can easily generate a SelectableScheme for the given
			//   column.  This doesn't work for the non-constant set.

			if (orderedBySelectColumn)
				table.OptimisedPostSet(column);

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this,
				            table + " = " + this + ".SimpleSelect(" + columnName + " " + debugSelectWith + " )");
#endif

			return table;
		}

		/// <summary>
		/// Exhaustively searches through this table for rows that match 
		/// the expression given.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="exp"></param>
		/// <remarks>
		/// This is the slowest type of query and is not able to use any 
		/// type of indexing.
		/// <para>
		/// A <see cref="IQueryContext"/> object is used for resolving 
		/// sub-query plans.  If there are no sub-query plans in the 
		/// expression, this can safely be 'null'.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table ExhaustiveSelect(IQueryContext context, Expression exp) {
			Table result = this;

			// Exit early if there's nothing in the table to select from
			int rowCount = RowCount;
			if (rowCount > 0) {
				TableVariableResolver resolver = GetVariableResolver();
				IRowEnumerator e = GetRowEnumerator();

				IntegerVector selectedSet = new IntegerVector(rowCount);

				while (e.MoveNext()) {
					int rowIndex = e.RowIndex;
					resolver.SetId = rowIndex;

					// Resolve expression into a constant.
					TObject rhsVal = exp.Evaluate(resolver, context);

					// If resolved to true then include in the selected set.
					if (!rhsVal.IsNull && rhsVal.TType is TBooleanType &&
						rhsVal.Object.Equals(true)) {
						selectedSet.AddInt(rowIndex);
					}
				}

				// Make into a table to return.
				VirtualTable table = new VirtualTable(this);
				table.Set(this, selectedSet);

				result = table;
			}

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, result + " = " + this + ".ExhaustiveSelect(" + exp + " )");
#endif

			return result;
		}

		/// <summary>
		/// Returns an array that represents the sorted order of this table by
		/// the given column number.
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public IntegerVector SelectAll(int columnIndex) {
			SelectableScheme ss = GetSelectableSchemeFor(columnIndex, columnIndex, this);
			return ss.SelectAll();
		}

		/// <summary>
		/// Evaluates a non-correlated ANY type operator given the LHS 
		/// expression, the RHS subquery and the ANY operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="expression">The left has side expression. The <see cref="VariableName"/>
		/// objects in this expression must all reference columns in this table.</param>
		/// <param name="op">The operator to use.</param>
		/// <param name="rightTable">The subquery table should only contain 
		/// on column.</param>
		/// <remarks>
		/// ANY creates a new table that contains only the rows in this 
		/// table that the expression and operator evaluate to true for 
		/// any values in the given table.
		/// <para>
		/// The IN operator can be represented by using '= ANY'.
		/// </para>
		/// <para>
		/// Note that unlike the other join and select methods in this 
		/// object this will take a complex expression as the lhs provided 
		/// all the <see cref="VariableName"/> objects resolve to this table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the result of the ANY function on the table.
		/// </returns>
		public virtual Table Any(IQueryContext context, Expression expression, Operator op, Table rightTable) {
			Table table = rightTable;

			// Check the table only has 1 column
			if (table.ColumnCount != 1)
				throw new ApplicationException("Input table <> 1 columns.");

			// Handle trivial case of no entries to select from
			if (RowCount == 0)
				return this;

			// If 'table' is empty then we return an empty set.  ANY { empty set } is
			// always false.
			if (table.RowCount == 0)
				return EmptySelect();

			// Is the lhs expression a constant?
			if (expression.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject lhsConst = expression.Evaluate(null, context);

				// Select from the table.
				IntegerVector ivec = table.SelectRows(0, op, lhsConst);
				if (ivec.Count > 0)
					// There's some entries so return the whole table,
					return this;

				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table sourceTable;
			int lhsColIndex;
			// Is the lhs expression a single variable?
			VariableName lhsVar = expression.VariableName;
			// NOTE: It'll be less common for this part to be called.
			if (lhsVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
				FunctionTable funTable = new FunctionTable(this, new Expression[] { expression }, new String[] { "1" }, dbContext);
				sourceTable = funTable;
				lhsColIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				lhsColIndex = sourceTable.FindFieldName(lhsVar);
				if (lhsColIndex == -1)
					throw new ApplicationException("Can't find column '" + lhsVar + "'.");
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataTableColumnInfo sourceCol = sourceTable.GetColumn(lhsColIndex);
			DataTableColumnInfo destCol = table.GetColumn(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
											   sourceCol.SQLTypeString + " is incompatible " +
											   "with the sub-query " + destCol.SQLTypeString +
											   ".");
			}

			// We now have all the information to solve this query.
			// We work output as follows:
			//   For >, >= type ANY we find the lowest value in 'table' and
			//   select from 'source' all the rows that are >, >= than the
			//   lowest value.
			//   For <, <= type ANY we find the highest value in 'table' and
			//   select from 'source' all the rows that are <, <= than the
			//   highest value.
			//   For = type ANY we use same method from INHelper.
			//   For <> type ANY we iterate through 'source' only including those
			//   rows that a <> query on 'table' returns size() != 0.

			IntegerVector selectVec;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCellContent(0);
				// Select from the source table all rows that are > or >= to the
				// lowest cell,
				selectVec = sourceTable.SelectRows(lhsColIndex, op, lowestCell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCellContent(0);
				// Select from the source table all rows that are < or <= to the
				// highest cell,
				selectVec = sourceTable.SelectRows(lhsColIndex, op, highestCell);
			} else if (op.IsEquivalent("=")) {
				// Equiv. to IN
				selectVec = INHelper.In(sourceTable, table, lhsColIndex, 0);
			} else if (op.IsEquivalent("<>")) {
				// Select the value that is the same of the entire column
				TObject cell = table.GetSingleCellContent(0);
				if (cell != null) {
					// All values from 'source_table' that are <> than the given cell.
					selectVec = sourceTable.SelectRows(lhsColIndex, op, cell);
				} else {
					// No, this means there are different values in the given set so the
					// query evaluates to the entire table.
					return this;
				}
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ANY.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, selectVec);

			// Query logging information
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this,
				            rtable + " = " + this + ".Any(" + expression + ", " + op + ", " + rightTable + ")");

			return rtable;
		}

		/// <summary>
		/// Evaluates a non-correlated ALL type operator given the LHS expression,
		/// the RHS subquery and the ALL operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="expression">Expression containing <see cref="VariableName"/> 
		/// objects referencing columns in this table.</param>
		/// <param name="op">The operator to use.</param>
		/// <param name="table">The subquery table containing only one column.</param>
		/// <remarks>
		/// For example: <c>Table.col > ALL ( SELECT .... )</c>
		/// <para>
		/// ALL creates a new table that contains only the rows in this table that
		/// the expression and operator evaluate to true for all values in the
		/// given table.
		/// </para>
		/// <para>
		/// The <c>NOT IN</c> operator can be represented by using <c>&lt;&gt; ALL'</c>.
		/// </para>
		/// <para>
		/// Note that unlike the other join and select methods in this object this
		/// will take a complex expression as the lhs provided all the Variable
		/// objects resolve to this table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the result of the ALL function on the table.
		/// </returns>
		public virtual Table All(IQueryContext context, Expression expression, Operator op, Table table) {
			// Check the table only has 1 column
			if (table.ColumnCount != 1)
				throw new ApplicationException("Input table <> 1 columns.");

			// Handle trivial case of no entries to select from
			if (RowCount == 0)
				return this;

			// If 'table' is empty then we return the complete set.  ALL { empty set }
			// is always true.
			if (table.RowCount == 0)
				return this;

			// Is the lhs expression a constant?
			if (expression.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject lhsConst = expression.Evaluate(null, context);
				bool comparedToTrue;
				// The various operators
				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Find the maximum value in the table
					TObject cell = table.GetLastCellContent(0);
					comparedToTrue = CompareCells(lhsConst, cell, op);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Find the minimum value in the table
					TObject cell = table.GetFirstCellContent(0);
					comparedToTrue = CompareCells(lhsConst, cell, op);
				} else if (op.IsEquivalent("=")) {
					// Only true if rhs is a single value
					TObject cell = table.GetSingleCellContent(0);
					comparedToTrue = (cell != null && CompareCells(lhsConst, cell, op));
				} else if (op.IsEquivalent("<>")) {
					// true only if lhs_cell is not found in column.
					comparedToTrue = !table.ColumnContainsCell(0, lhsConst);
				} else {
					throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
				}

				// If matched return this table
				if (comparedToTrue)
					return this;

				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table sourceTable;
			int lhsColIndex;
			// Is the lhs expression a single variable?
			VariableName lhsVar = expression.VariableName;
			// NOTE: It'll be less common for this part to be called.
			if (lhsVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
				FunctionTable funTable = new FunctionTable(this, new Expression[] { expression }, new String[] { "1" }, dbContext);
				sourceTable = funTable;
				lhsColIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				lhsColIndex = sourceTable.FindFieldName(lhsVar);
				if (lhsColIndex == -1)
					throw new ApplicationException("Can't find column '" + lhsVar + "'.");
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataTableColumnInfo sourceCol = sourceTable.GetColumn(lhsColIndex);
			DataTableColumnInfo destCol = table.GetColumn(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
											   sourceCol.SQLTypeString + " is incompatible " +
											   "with the sub-query " + destCol.SQLTypeString +
											   ".");
			}

			// We now have all the information to solve this query.
			// We work output as follows:
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

			IntegerVector selectVec;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCellContent(0);
				// Select from the source table all rows that are > or >= to the
				// highest cell,
				selectVec = sourceTable.SelectRows(lhsColIndex, op, highestCell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCellContent(0);
				// Select from the source table all rows that are < or <= to the
				// lowest cell,
				selectVec = sourceTable.SelectRows(lhsColIndex, op, lowestCell);
			} else if (op.IsEquivalent("=")) {
				// Select the single value from the set (if there is one).
				TObject singleCell = table.GetSingleCellContent(0);
				if (singleCell != null) {
					// Select all from source_table all values that = this cell
					selectVec = sourceTable.SelectRows(lhsColIndex, op, singleCell);
				} else {
					// No single value so return empty set (no value in LHS will equal
					// a value in RHS).
					return EmptySelect();
				}
			} else if (op.IsEquivalent("<>")) {
				// Equiv. to NOT IN
				selectVec = INHelper.NotIn(sourceTable, table, lhsColIndex, 0);
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, selectVec);

			// Query logging information
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this,
				            rtable + " = " + this + ".All(" + expression + ", " + op + ", " + table + ")");

			return rtable;
		}
	}
}