using System;
using System.Collections.Generic;

using Deveel.Data.Types;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data.DbSystem {
	public abstract partial class Table {
		/// <summary>
		/// Compares two instances with the given operator.
		/// </summary>
		/// <param name="ob1">First value to compare.</param>
		/// <param name="op">Operator for the comparation.</param>
		/// <param name="ob2">Second value to compare.</param>
		/// <returns>
		/// Returns a boolean value if the evaluation with the given
		/// operator of the two values is <see cref="Boolean"/>, 
		/// otherwise throw an exception.
		/// </returns>
		/// <exception cref="NullReferenceException">If the value returned by
		/// the evaluation is not a <see cref="Boolean"/>.</exception>
		private static bool CompareCells(TObject ob1, TObject ob2, Operator op) {
			TObject result = op.Evaluate(ob1, ob2, null, null, null);
			// NOTE: This will be a NullPointerException if the result is not a
			//   boolean type.
			//TODO: check...
			bool? bresult = result.ToNullableBoolean();
			if (!bresult.HasValue)
				throw new NullReferenceException();
			return bresult.Value;
		}

		/// <summary>
		/// Returns true if the given column contains all values that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		internal bool AllColumnMatchesValue(int column, Operator op, TObject ob) {
			IList<int> rows = SelectRows(column, op, ob);
			return (rows.Count == RowCount);
		}

		/// <summary>
		/// Select all the rows of the table matching the given values for the
		/// given columns.
		/// </summary>
		/// <param name="cols"></param>
		/// <param name="op"></param>
		/// <param name="cells"></param>
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
		internal IList<int> SelectRows(int[] cols, Operator op, TObject[] cells) {
			// TODO: Look for an multi-column index to make this a lot faster,
			if (cols.Length > 1)
				throw new ApplicationException("Multi-column select not supported.");

			return SelectRows(cols[0], op, cells[0]);
		}

		/// <summary>
		/// Select all the rows of the table matching the given value for the
		/// given column.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="cell"></param>
		/// <returns>
		/// Returns a set that respresents the list of row numbers
		/// selected from the table given the condition.
		/// </returns>
		internal IList<int> SelectRows(int column, Operator op, TObject cell) {
			// If the cell is of an incompatible type, return no results,
			TType colType = GetTTypeForColumn(column);
			if (!cell.TType.IsComparableType(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			// Get the selectable scheme for this column
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);

			// If the operator is a standard operator, use the interned SelectableScheme
			// methods.
			if (op.IsEquivalent("="))
				return ss.SelectEqual(cell);
			if (op.IsEquivalent("<>"))
				return ss.SelectNotEqual(cell);
			if (op.IsEquivalent(">"))
				return ss.SelectGreater(cell);
			if (op.IsEquivalent("<"))
				return ss.SelectLess(cell);
			if (op.IsEquivalent(">="))
				return ss.SelectGreaterOrEqual(cell);
			if (op.IsEquivalent("<="))
				return ss.SelectLessOrEqual(cell);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			SelectableRangeSet rangeSet = new SelectableRangeSet();
			rangeSet.Intersect(op, cell);
			return ss.SelectRange(rangeSet.ToArray());
		}

		/// <summary>
		/// Selects the rows in a table column between two minimum and maximum 
		/// bounds.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="minCell"></param>
		/// <param name="maxCell"></param>
		/// <remarks>
		/// <b>Note</b> The returns IntegerList <b>must</b> be sorted be the 
		/// <paramref name="column"/> cells.
		/// </remarks>
		/// <returns>
		/// Returns all the rows in the table with the value of <paramref name="column"/>
		/// column greater or equal then <paramref name="minCell"/> and smaller then
		/// <paramref name="maxCell"/>.
		/// </returns>
		public IList<int> SelectBetween(int column, TObject minCell, TObject maxCell) {
			// Check all the tables are comparable
			TType colType = GetTTypeForColumn(column);
			if (!minCell.TType.IsComparableType(colType) ||
				!maxCell.TType.IsComparableType(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectBetween(minCell, maxCell);
		}

		/// <summary>
		/// Selects all the rows where the given column matches the regular
		/// expression.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to 
		/// perform the operation.
		/// <para>
		/// This method must guarentee the result is ordered by the given 
		/// column index.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectFromRegex(int column, Operator op, TObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return PatternSearch.RegexSearch(this, column, ob.Object.ToString());
		}

		/// <summary>
		/// Selects all the rows where the given column matches the 
		/// given pattern.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op">Operator for the selection (either <c>LIKE</c> 
		/// or <c>NOT LIKE</c>).</param>
		/// <param name="ob"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to perform 
		/// these operations.
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectFromPattern(int column, Operator op, TObject ob) {
			if (ob.IsNull)
				return new List<int>();

			if (op.IsEquivalent("not like")) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.
				List<int> likeSet = (List<int>)PatternSearch.Search(this, column, ob.ToString());
				// Don't include NULL values
				TObject nullCell = new TObject(ob.TType, null);
				IList<int> originalSet = SelectRows(column, Operator.Get("is not"), nullCell);
				int listSize = SysMath.Max(4, (originalSet.Count - likeSet.Count) + 4);
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
			return PatternSearch.Search(this, column, ob.ToString());
		}

		/// <summary>
		/// Returns a new table based on this table with no rows in it.
		/// </summary>
		/// <returns></returns>
		public Table EmptySelect() {
			if (RowCount == 0)
				return this;

			VirtualTable table = new VirtualTable(this);
			table.Set(this, new List<int>(0));
			return table;
		}

		/// <summary>
		/// Selects a single row at the given index from this table.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <returns></returns>
		public Table SingleRowSelect(int rowIndex) {
			VirtualTable table = new VirtualTable(this);
			List<int> ivec = new List<int>(1);
			ivec.Add(rowIndex);
			table.Set(this, ivec);
			return table;
		}

		public Table RangeSelect(string columnName, SelectableRange[] ranges) {
			return RangeSelect(ResolveColumnName(columnName), ranges);
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
				ranges[0].Equals(SelectableRange.FullRange))
				// Yes, so return this table.
				return this;

			// Must be a non-trivial range selection.

			// Find the column index of the column selected
			int column = FindFieldName(columnName);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the column given to select the range of: " +
				   columnName.Name);
			}

			// Select the range
			IList<int> rows = SelectRange(column, ranges);

			// Make a new table with the range selected
			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// We know the new set is ordered by the column.
			table.OptimisedPostSet(column);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".RangeSelect(" + columnName + ", " + ranges + " )");
#endif

			return table;

		}

		/// <summary>
		/// A simple select on this table.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="columnName">The left has side column reference.</param>
		/// <param name="op">The operator.</param>
		/// <param name="rhs">The expression to select against (the 
		/// expression <b>must</b> be a constant).</param>
		/// <remarks>
		/// We select against a column, with an <see cref="Operator"/> and a 
		/// rhs <see cref="Expression"/> that is constant (only needs to be 
		/// evaluated once).
		/// </remarks>
		/// <returns></returns>
		public Table SimpleSelect(IQueryContext context, VariableName columnName, Operator op, Expression rhs) {
			string debugSelectWith;

			// Find the row with the name given in the condition.
			int column = FindFieldName(columnName);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the LHS column specified in the condition: " +
				   columnName.Name);
			}

			IList<int> rows;

			bool orderedBySelectColumn;

			// If we are doing a sub-query search
			if (op.IsSubQuery) {

				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				object ob = rhs.Last;
				if (!(ob is TObject))
					throw new Exception("Sub-query not a TObject");

				TObject tob = (TObject)ob;
				if (tob.TType is TArrayType) {
					Expression[] list = (Expression[])tob.Object;

					// Construct a temporary table with a single column that we are
					// comparing to.
					DataColumnInfo col = GetColumnInfo(FindFieldName(columnName));
					DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
					TemporaryTable ttable = new TemporaryTable(dbContext.Database, "single", new DataColumnInfo[] {col});

					foreach (Expression expression in list) {
						ttable.NewRow();
						ttable.SetRowObject(expression.Evaluate(null, null, context), 0);
					}

					ttable.SetupAllSelectableSchemes();

					// Perform the any/all sub-query on the constant table.

					return TableFunctions.AnyAllNonCorrelated(this, new VariableName[] { columnName }, op, ttable);

				}

				throw new Exception("Error with format or RHS expression.");
			}

			// If we are doing a LIKE or REGEX pattern search
			if (op.IsEquivalent("like") || 
				op.IsEquivalent("not like") || 
				op.IsEquivalent("regex")) {

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject value = rhs.Evaluate(null, context);

				if (op.IsEquivalent("regex")) {
					// Use the regular expression search to determine matching rows.
					rows = SelectFromRegex(column, op, value);
				} else {
					// Use the standard SQL pattern matching engine to determine
					// matching rows.
					rows = SelectFromPattern(column, op, value);
				}

				// These searches guarentee result is ordered by the column
				orderedBySelectColumn = true;

				// Describe the 'LIKE' select
#if DEBUG
				debugSelectWith = op + " " + value;
#endif

			}
				// Otherwise, we doing an index based comparison.
			else {

				// Is the column we are searching on indexable?
				DataColumnInfo colInfo = GetColumnInfo(column);
				if (!colInfo.IsIndexableType) {
					throw new StatementException("Can not search on field type " +
												 colInfo.TType.ToSqlString() +
												 " in '" + colInfo.Name + "'");
				}

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject value = rhs.Evaluate(null, context);

				// Get the rows of the selected set that match the given condition.
				rows = SelectRows(column, op, value);
				orderedBySelectColumn = true;

				// Describe the select
#if DEBUG
				debugSelectWith = op + " " + value;
#endif

			}

			// We now has a set of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// OPTIMIZATION: Since we know that the 'select' return is ordered by the
			//   LHS column, we can easily generate a SelectableScheme for the given
			//   column.  This doesn't work for the non-constant set.

			if (orderedBySelectColumn) {
				table.OptimisedPostSet(column);
			}

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".SimpleSelect(" + columnName + " " + debugSelectWith + " )");
#endif

			return table;

		}

		/// <summary>
		/// Exhaustively searches through this table for rows that match 
		/// the expression given.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="expression"></param>
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
		public Table ExhaustiveSelect(IQueryContext context, Expression expression) {
			Table result = this;

			// Exit early if there's nothing in the table to select from
			int rowCount = RowCount;
			if (rowCount > 0) {
				TableVariableResolver resolver = GetVariableResolver();
				IRowEnumerator e = GetRowEnumerator();

				List<int> selectedSet = new List<int>(rowCount);

				while (e.MoveNext()) {
					int rowIndex = e.RowIndex;
					resolver.SetId = rowIndex;

					// Resolve expression into a constant.
					TObject value = expression.Evaluate(resolver, context);

					// If resolved to true then include in the selected set.
					if (!value.IsNull && value.TType is TBooleanType &&
						value.Object.Equals(true)) {
						selectedSet.Add(rowIndex);
					}
				}

				// Make into a table to return.
				VirtualTable table = new VirtualTable(this);
				table.Set(this, selectedSet);

				result = table;
			}

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, result + " = " + this + ".ExhaustiveSelect(" + expression + " )");
#endif

			return result;
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
		public Table Any(IQueryContext context, Expression expression, Operator op, Table rightTable) {
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
				TObject value = expression.Evaluate(null, context);
				// Select from the table.
				IList<int> list = table.SelectRows(0, op, value);
				if (list.Count > 0)
					// There's some entries so return the whole table,
					return this;

				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table sourceTable;
			int lhsColIndex;
			// Is the lhs expression a single variable?
			VariableName expVar = expression.AsVariableName();
			// NOTE: It'll be less common for this part to be called.
			if (expVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				FunctionTable funTable = new FunctionTable(this, new Expression[] { expression }, new String[] { "1" }, context);
				sourceTable = funTable;
				lhsColIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				lhsColIndex = sourceTable.FindFieldName(expVar);
				if (lhsColIndex == -1) {
					throw new ApplicationException("Can't find column '" + expVar + "'.");
				}
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataColumnInfo sourceCol = sourceTable.GetColumnInfo(lhsColIndex);
			DataColumnInfo destCol = table.GetColumnInfo(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
								sourceCol.TType.ToSqlString() + " is incompatible " +
								"with the sub-query " + destCol.TType.ToSqlString() +
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

			IList<int> selectRows;
			if (op.IsEquivalent(">") || 
				op.IsEquivalent(">=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCell(0);
				// Select from the source table all rows that are > or >= to the
				// lowest cell,
				selectRows = sourceTable.SelectRows(lhsColIndex, op, lowestCell);
			} else if (op.IsEquivalent("<") || 
				op.IsEquivalent("<=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCell(0);
				// Select from the source table all rows that are < or <= to the
				// highest cell,
				selectRows = sourceTable.SelectRows(lhsColIndex, op, highestCell);
			} else if (op.IsEquivalent("=")) {
				// Equiv. to IN
				selectRows = InHelper.In(sourceTable, table, lhsColIndex, 0);
			} else if (op.IsEquivalent("<>")) {
				// Select the value that is the same of the entire column
				TObject cell = table.GetSingleCell(0);
				if (cell != null) {
					// All values from 'source_table' that are <> than the given cell.
					selectRows = sourceTable.SelectRows(lhsColIndex, op, cell);
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
			rtable.Set(this, selectRows);

#if DEBUG
			// Query logging information
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, rtable + " = " + this + ".Any(" + expression + ", " + op + ", " + rightTable + ")");
#endif

			return rtable;
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are also in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public IList<int> AllIn(int column, Table table) {
			return InHelper.In(this, table, column, 0);
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are not in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public IList<int> AllNotIn(int column, Table table) {
			return InHelper.NotIn(this, table, column, 0);
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
		public Table All(IQueryContext context, Expression expression, Operator op, Table table) {
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
				TObject value = expression.Evaluate(null, context);
				bool comparedToTrue;

				// The various operators
				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Find the maximum value in the table
					TObject cell = table.GetLastCell(0);
					comparedToTrue = CompareCells(value, cell, op);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Find the minimum value in the table
					TObject cell = table.GetFirstCell(0);
					comparedToTrue = CompareCells(value, cell, op);
				} else if (op.IsEquivalent("=")) {
					// Only true if rhs is a single value
					TObject cell = table.GetSingleCell(0);
					comparedToTrue = (cell != null && CompareCells(value, cell, op));
				} else if (op.IsEquivalent("<>")) {
					// true only if lhs_cell is not found in column.
					comparedToTrue = !table.ColumnContainsValue(0, value);
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
			int colIndex;
			// Is the lhs expression a single variable?
			VariableName expVar = expression.AsVariableName();
			// NOTE: It'll be less common for this part to be called.
			if (expVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
				FunctionTable funTable = new FunctionTable(
					  this, new Expression[] { expression }, new String[] { "1" }, dbContext);
				sourceTable = funTable;
				colIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				colIndex = sourceTable.FindFieldName(expVar);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column '" + expVar + "'.");
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataColumnInfo sourceCol = sourceTable.GetColumnInfo(colIndex);
			DataColumnInfo destCol = table.GetColumnInfo(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType))
				throw new ApplicationException("The type of the sub-query expression " +
				                               sourceCol.TType.ToSqlString() + " is incompatible " +
				                               "with the sub-query " + destCol.TType.ToSqlString() +
				                               ".");

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

			IList<int> selectList;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCell(0);
				// Select from the source table all rows that are > or >= to the
				// highest cell,
				selectList = sourceTable.SelectRows(colIndex, op, highestCell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCell(0);
				// Select from the source table all rows that are < or <= to the
				// lowest cell,
				selectList = sourceTable.SelectRows(colIndex, op, lowestCell);
			} else if (op.IsEquivalent("=")) {
				// Select the single value from the set (if there is one).
				TObject singleCell = table.GetSingleCell(0);
				if (singleCell != null) {
					// Select all from source_table all values that = this cell
					selectList = sourceTable.SelectRows(colIndex, op, singleCell);
				} else {
					// No single value so return empty set (no value in LHS will equal
					// a value in RHS).
					return EmptySelect();
				}
			} else if (op.IsEquivalent("<>")) {
				// Equiv. to NOT IN
				selectList = InHelper.NotIn(sourceTable, table, colIndex, 0);
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, selectList);

#if DEBUG
			// Query logging information
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, rtable + " = " + this + ".All(" + expression + ", " + op + ", " + table + ")");
#endif

			return rtable;
		}

		/// <summary>
		/// Returns an array that represents the sorted order of this table by
		/// the given column number.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectAll(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectAll();
		}

		/// <summary>
		/// Returns a list of rows that represents the enumerator order of 
		/// this table.
		/// </summary>
		/// <returns></returns>
		public IList<int> SelectAll() {
			List<int> list = new List<int>(RowCount);
			IRowEnumerator en = GetRowEnumerator();
			while (en.MoveNext()) {
				list.Add(en.RowIndex);
			}
			return list;
		}

		/// <summary>
		/// Returns a list that represents the sorted order of this table of all
		/// values in the given SelectableRange objects of the given column index.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="ranges"></param>
		/// <remarks>
		/// If there is an index on the column, the result can be found very quickly.
		/// The range array must be normalized (no overlapping ranges).
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectRange(int column, SelectableRange[] ranges) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectRange(ranges);
		}

		/// <summary>
		/// Returns a list that represents the last sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectLast(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectLast();
		}

		/// <summary>
		/// Returns a list that represents the first sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectFirst(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectFirst();
		}

		/// <summary>
		/// Returns a list that represents the rest of the sorted element(s) of the
		/// given column index (not the <i>first</i> set).
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectRest(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
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
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, tableOut + " = " + this + ".Distinct()");
#endif

			return tableOut;
		}

		/// <summary>
		/// Returns a new table that has only distinct rows in it.
		/// </summary>
		/// <param name="columns">Integer array containing the columns 
		/// to make distinct over.</param>
		/// <remarks>
		/// This is an expensive operation. We sort over all the columns, then 
		/// iterate through the result taking out any duplicate rows.
		/// <para>
		/// <b>Note</b>: This will change the order of this table in the result.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table Distinct(int[] columns) {
			List<int> resultList = new List<int>();
			IList<int> rowList = OrderedRowList(columns);

			int rowCount = rowList.Count;
			int previousRow = -1;
			for (int i = 0; i < rowCount; ++i) {
				int rowIndex = rowList[i];

				if (previousRow != -1) {

					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < columns.Length && equal; ++n) {
						TObject c1 = GetCell(columns[n], rowIndex);
						TObject c2 = GetCell(columns[n], previousRow);
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
			VirtualTable vt = new VirtualTable(this);
			vt.Set(this, resultList);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, vt + " = " + this + ".distinct(" + columns + ")");
#endif

			return vt;
		}
	}
}
