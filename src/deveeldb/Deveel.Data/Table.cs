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
using System.Collections;
using System.IO;

using Deveel.Data.Collections;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data {
	/// <summary>
	/// This is a definition for a table in the database.
	/// </summary>
	/// <remarks>
	/// It stores the name of the table, and the fields (columns) in the 
	/// table.  A table represents either a 'core' <see cref="DataTable"/>
	/// that directly maps to the information stored in the database, or a 
	/// temporary table generated on the fly.
	/// <para>
	/// It is an abstract class, because it does not implement the methods to 
	/// add, remove or access row data in the table.
	/// </para>
	/// </remarks>
	public abstract class Table : ITableDataSource {

		// Set to true to output query debugging information.  All table operation
		// commands will be output.
		protected static bool DEBUG_QUERY = true;


		/// <summary>
		/// Returns the <see cref="Database"/> object that this table is derived from.
		/// </summary>
		public abstract Database Database { get; }

		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> object that this table is part of.
		/// </summary>
		public TransactionSystem System {
			get { return Database.System; }
		}

		/// <summary>
		/// Returns a <see cref="IDebugLogger"/> object that we can use to log 
		/// debug messages to.
		/// </summary>
		protected internal virtual IDebugLogger Debug {
			get { return System.Debug; }
		}

		/// <summary>
		/// Returns the number of columns in the table.
		/// </summary>
		public abstract int ColumnCount { get; }

		/// <summary>
		/// Returns the number of rows stored in the table.
		/// </summary>
		public abstract int RowCount { get; }

		/// <summary>
		/// Returns a <see cref="TType"/> object that would represent 
		/// values at the given column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">
		/// If the column can't be found.
		/// </exception>
		public TType GetTTypeForColumn(int column) {
			return DataTableDef[column].TType;
		}

		/// <summary>
		/// Returns a <see cref="TType"/> object that would represent 
		/// values in the given column.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">
		/// If the column can't be found.
		/// </exception>
		public TType GetTTypeForColumn(VariableName v) {
			return GetTTypeForColumn(FindFieldName(v));
		}

		/// <summary>
		/// Given a fully qualified variable field name, this will 
		/// return the column index the field is at.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public abstract int FindFieldName(VariableName v);


		/// <summary>
		/// Returns a fully qualified <see cref="VariableName"/> object 
		/// that represents the name of the column at the given index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public abstract VariableName GetResolvedVariable(int column);

		/// <summary>
		/// Returns a <see cref="SelectableScheme"/> for the given column 
		/// in the given <see cref="VirtualTable"/> row domain.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="original_column"></param>
		/// <param name="table"></param>
		/// <remarks>
		/// The <paramref name="column"/> variable may be modified as it traverses 
		/// through the tables, however the <paramref name="original_column"/>
		/// retains the link to the column in <paramref name="table"/>.
		/// </remarks>
		/// <returns></returns>
		internal abstract SelectableScheme GetSelectableSchemeFor(int column, int original_column, Table table);

		/// <summary>
		/// Given a set, this trickles down through the <see cref="Table"/> 
		/// hierarchy resolving the given row_set to a form that the given 
		/// ancestor understands.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row_set"></param>
		/// <param name="ancestor"></param>
		/// <remarks>
		/// Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
		/// down three levels and return a new 7 element set with the rows fully
		/// resolved to the given ancestors domain.
		/// </remarks>
		internal abstract void SetToRowTableDomain(int column, IntegerVector row_set, ITableDataSource ancestor);

		/// <summary>
		/// Return the list of <see cref="DataTable"/> and row sets that make up 
		/// the raw information in this table.
		/// </summary>
		/// <param name="info"></param>
		/// <returns></returns>
		internal abstract RawTableInformation ResolveToRawTable(RawTableInformation info);

		/// <summary>
		/// Returns an object that represents the information in the given 
		/// cell in the table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// This will generally be an expensive algorithm, so calls to it should 
		/// be kept to a minimum.  Note that the offset between two rows is not 
		/// necessarily 1. Use <see cref="GetRowEnumerator"/> to get the contents 
		/// of a set.
		/// </remarks>
		/// <returns></returns>
		public abstract TObject GetCellContents(int column, int row);

		/// <summary>
		/// Returns an <see cref="IEnumerator"/> of the rows in this table.
		/// </summary>
		/// <remarks>
		/// Each call to <see cref="IRowEnumerator.RowIndex"/> returns the 
		/// next valid row in the table. Note that the order that rows are retreived 
		/// depend on a number of factors. For a <see cref="DataTable"/> the rows are 
		/// accessed in the order they are in the data file.  For a <see cref="VirtualTable"/>, 
		/// the rows are accessed in the order of the last select operation.
		/// <para>
		/// If you want the rows to be returned by a specific column order then 
		/// use the <i>Sselec*</i> methods.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public abstract IRowEnumerator GetRowEnumerator();

		/// <summary>
		/// Returns a <see cref="DataTableDef"/> object that defines the name 
		/// of the table and the layout of the columns of the table.
		/// </summary>
		/// <remarks>
		/// Note that for tables that are joined with other tables, the table name 
		/// and schema for this object become mangled.  For example, a table called 
		/// <c>PERSON</c> joined with a table called <c>MUSIC</c> becomes a table 
		/// called <c>PERSON#MUSIC</c> in a null schema.
		/// </remarks>
		public abstract DataTableDef DataTableDef { get; }

		/// <summary>
		/// Adds a <see cref="IDataTableListener"/> to the <see cref="DataTable"/>
		/// objects at the root of this table tree hierarchy.
		/// </summary>
		/// <param name="listener"></param>
		/// <remarks>
		/// If this table represents the join of a number of tables then the 
		/// <see cref="IDataTableListener"/> is added to all the <see cref="DataTable"/>
		/// objects at the root.
		/// <para>
		/// A <see cref="IDataTableListener"/> is notified of all modifications to the 
		/// raw entries of the table.  This listener can be used for detecting changes 
		/// in VIEWs, for triggers or for caching of common queries.
		/// </para>
		/// </remarks>
		internal abstract void AddDataTableListener(IDataTableListener listener);

		/// <summary>
		/// Removes a <see cref="IDataTableListener"/> from the <see cref="DataTable"/> 
		/// objects at the root of this table tree hierarchy.
		/// </summary>
		/// <param name="listener"></param>
		/// <remarks>
		/// If this table represents the join of a number of tables, then the 
		/// <see cref="IDataTableListener"/> is removed from all the <see cref="DataTable"/> 
		/// objects at the root.
		/// </remarks>
		internal abstract void RemoveDataTableListener(IDataTableListener listener);

		/// <summary>
		/// Locks the root table(s) of this table so that it is impossible to
		/// overwrite the underlying rows that may appear in this table.
		/// </summary>
		/// <param name="lock_key">A given key that will also unlock the root table(s).</param>
		/// <remarks>
		/// This is used when cells in the table need to be accessed 'outside' 
		/// the Lock.  So we may have late access to cells in the table.
		/// <para>
		/// <b>Note</b>: This is nothing to do with the <see cref="LockingMechanism"/> object.
		/// </para>
		/// </remarks>
		public abstract void LockRoot(int lock_key);

		/// <summary>
		/// Unlocks the root tables so that the underlying rows may
		/// once again be used if they are not locked and have been removed.
		/// </summary>
		/// <param name="lock_key"></param>
		/// <remarks>
		/// This should be called some time after the rows have been locked.
		/// </remarks>
		public abstract void UnlockRoot(int lock_key);

		/// <summary>
		/// Returns true if the table has its row roots locked 
		/// (via the <see cref="LockRoot"/> method.
		/// </summary>
		public abstract bool HasRootsLocked { get; }

		// ---------- Implemented from ITableDataSource ----------

		/// <summary>
		/// Returns the <see cref="SelectableScheme"/> that indexes the 
		/// given column in this table.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public SelectableScheme GetColumnScheme(int column) {
			return GetSelectableSchemeFor(column, column, this);
		}


		// ---------- Convenience methods ----------

		/// <summary>
		/// Returns the <see cref="DataTableColumnDef"/> object for the 
		/// given column index.
		/// </summary>
		/// <param name="col_index"></param>
		/// <returns></returns>
		public DataTableColumnDef GetColumnDef(int col_index) {
			return DataTableDef[col_index];
		}


		/** ======================= Table Operations ========================= */

		///<summary>
		/// Dumps the contents of the table in a human readable form to the 
		/// given output stream.
		///</summary>
		///<param name="output"></param>
		/// <remarks>
		/// This should only be used for debuging the database.
		/// </remarks>
		public void DumpTo(TextWriter output) {
			DumpHelper.Dump(this, output);
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
		/// Returns a table that is a merge of this table and the destination table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// The rows that are in the destination table are included in this table.
		/// <para>
		/// The tables must have the same number of rows.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table ColumnMerge(Table table) {
			if (RowCount != table.RowCount) {
				throw new ApplicationException("Tables have different row counts.");
			}
			// Create the new VirtualTable with the joined tables.

			IntegerVector all_row_set = new IntegerVector();
			int rcount = RowCount;
			for (int i = 0; i < rcount; ++i) {
				all_row_set.AddInt(i);
			}

			Table[] tabs = new Table[] { this, table };
			IntegerVector[] row_sets = new IntegerVector[] { all_row_set, all_row_set };

			VirtualTable out_table = new VirtualTable(tabs);
			out_table.Set(tabs, row_sets);

			return out_table;
		}


		// ---------- Queries using Expression class ----------

		/// <summary>
		/// A single column range select on this table.
		/// </summary>
		/// <param name="col_var">The column variable in this table (eg. Part.id)</param>
		/// <param name="ranges">The normalized (no overlapping) set of ranges to find.</param>
		/// <remarks>
		/// This can often be solved very quickly especially if there is an index 
		/// on the column.  The <see cref="SelectableRange"/> array represents a 
		/// set of ranges that are returned that meet the given criteria.
		/// </remarks>
		/// <returns></returns>
		public Table RangeSelect(VariableName col_var, SelectableRange[] ranges) {
			// If this table is empty then there is no range to select so
			// trivially return this object.
			if (RowCount == 0) {
				return this;
			}
			// Are we selecting a black or null range?
			if (ranges == null || ranges.Length == 0) {
				// Yes, so return an empty table
				return EmptySelect();
			}
			// Are we selecting the entire range?
			if (ranges.Length == 1 &&
				ranges[0].Equals(SelectableRange.FULL_RANGE)) {
				// Yes, so return this table.
				return this;
			}

			// Must be a non-trivial range selection.

			// Find the column index of the column selected
			int column = FindFieldName(col_var);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the column given to select the range of: " +
				   col_var.Name);
			}

			// Select the range
			IntegerVector rows;
			rows = SelectRange(column, ranges);

			// Make a new table with the range selected
			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// We know the new set is ordered by the column.
			table.OptimisedPostSet(column);

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								table + " = " + this + ".RangeSelect(" +
								col_var + ", " + ranges + " )");
				}
			}

			return table;

		}

		/// <summary>
		/// A simple select on this table.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="lhs_var">The left has side column reference.</param>
		/// <param name="op">The operator.</param>
		/// <param name="rhs">The expression to select against (the 
		/// expression <b>must</b> be a constant).</param>
		/// <remarks>
		/// We select against a column, with an <see cref="Operator"/> and a 
		/// rhs <see cref="Expression"/> that is constant (only needs to be 
		/// evaluated once).
		/// </remarks>
		/// <returns></returns>
		public Table SimpleSelect(IQueryContext context, VariableName lhs_var, Operator op, Expression rhs) {
			String DEBUG_SELECT_WITH = null;

			// Find the row with the name given in the condition.
			int column = FindFieldName(lhs_var);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the LHS column specified in the condition: " +
				   lhs_var.Name);
			}

			IntegerVector rows;

			bool ordered_by_select_column;

			// If we are doing a sub-query search
			if (op.IsSubQuery) {

				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				Object ob = rhs.Last;
				if (!(ob is TObject)) {
					throw new Exception("Sub-query not a TObject");
				}
				TObject tob = (TObject)ob;
				if (tob.TType is TArrayType) {
					Expression[] list = (Expression[])tob.Object;

					// Construct a temporary table with a single column that we are
					// comparing to.
					TemporaryTable ttable;
					DataTableColumnDef col = GetColumnDef(FindFieldName(lhs_var));
					DatabaseQueryContext db_context = (DatabaseQueryContext)context;
					ttable = new TemporaryTable(db_context.Database,
											 "single", new DataTableColumnDef[] { col });

					for (int i = 0; i < list.Length; ++i) {
						ttable.NewRow();
						ttable.SetRowObject(list[i].Evaluate(null, null, context), 0);
					}
					ttable.SetupAllSelectableSchemes();

					// Perform the any/all sub-query on the constant table.

					return TableFunctions.AnyAllNonCorrelated(
						   this, new VariableName[] { lhs_var }, op, ttable);

				} else {
					throw new Exception("Error with format or RHS expression.");
				}

			}
				// If we are doing a LIKE or REGEX pattern search
			else if (op.IsEquivalent("like") || op.IsEquivalent("not like") || op.IsEquivalent("regex")) {

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject rhs_const = rhs.Evaluate(null, context);

				if (op.IsEquivalent("regex")) {
					// Use the regular expression search to determine matching rows.
					rows = SelectFromRegex(column, op, rhs_const);
				} else {
					// Use the standard SQL pattern matching engine to determine
					// matching rows.
					rows = SelectFromPattern(column, op, rhs_const);
				}
				// These searches guarentee result is ordered by the column
				ordered_by_select_column = true;

				// Describe the 'LIKE' select
				if (DEBUG_QUERY) {
					DEBUG_SELECT_WITH = op + " " + rhs_const;
				}

			}
				// Otherwise, we doing an index based comparison.
			else {

				// Is the column we are searching on indexable?
				DataTableColumnDef col_def = GetColumnDef(column);
				if (!col_def.IsIndexableType) {
					throw new StatementException("Can not search on field type " +
												 col_def.SQLTypeString +
												 " in '" + col_def.Name + "'");
				}

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject rhs_const = rhs.Evaluate(null, context);

				// Get the rows of the selected set that match the given condition.
				rows = SelectRows(column, op, rhs_const);
				ordered_by_select_column = true;

				// Describe the select
				if (DEBUG_QUERY) {
					DEBUG_SELECT_WITH = op + " " + rhs_const;
				}

			}

			// We now has a set of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// OPTIMIZATION: Since we know that the 'select' return is ordered by the
			//   LHS column, we can easily generate a SelectableScheme for the given
			//   column.  This doesn't work for the non-constant set.

			if (ordered_by_select_column) {
				table.OptimisedPostSet(column);
			}

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								table + " = " + this + ".SimpleSelect(" +
								lhs_var + " " + DEBUG_SELECT_WITH + " )");
				}
			}

			return table;

		}

		/// <summary>
		/// A simple join operation.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="lhs_var"></param>
		/// <param name="op"></param>
		/// <param name="rhs"></param>
		/// <remarks>
		/// A simple join operation is one that has a single joining operator, 
		/// a <see cref="VariableName"/> on the lhs and a simple expression on the 
		/// rhs that includes only columns in the rhs table. For example, 
		/// <c>id = part_id</c> or <c>id == part_id * 2</c> or <c>id == part_id + vendor_id * 2</c>
		/// <para>
		/// It is important to understand how this algorithm works because all
		/// optimization of the expression must happen before the method starts.
		/// </para>
		/// <para>
		/// The simple join algorithm works as follows:  Every row of the right hand
		/// side table 'table' is iterated through.  The select opreation is applied
		/// to this table given the result evaluation.  Each row that matches is
		/// included in the result table.
		/// </para>
		/// <para>
		/// For optimal performance, the expression should be arranged so that the rhs
		/// table is the smallest of the two tables (because we must iterate through
		/// all rows of this table).  This table should be the largest.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table SimpleJoin(IQueryContext context, Table table, VariableName lhs_var, Operator op, Expression rhs) {

			// Find the row with the name given in the condition.
			int lhs_column = FindFieldName(lhs_var);

			if (lhs_column == -1) {
				throw new Exception(
				   "Unable to find the LHS column specified in the condition: " +
				   lhs_var.ToString());
			}

			// Create a variable resolver that can resolve columns in the destination
			// table.
			TableVariableResolver resolver = table.GetVariableResolver();

			// The join algorithm.  It steps through the RHS expression, selecting the
			// cells that match the relation from the LHS table (this table).

			IntegerVector this_row_set = new IntegerVector();
			IntegerVector table_row_set = new IntegerVector();

			IRowEnumerator e = table.GetRowEnumerator();

			while (e.MoveNext()) {
				int row_index = e.RowIndex;
				resolver.SetId = row_index;

				// Resolve expression into a constant.
				TObject rhs_val = rhs.Evaluate(resolver, context);

				// Select all the rows in this table that match the joining condition.
				IntegerVector selected_set = SelectRows(lhs_column, op, rhs_val);

				// Include in the set.
				int size = selected_set.Count;
				for (int i = 0; i < size; ++i) {
					table_row_set.AddInt(row_index);
				}
				this_row_set.Append(selected_set);

			}

			// Create the new VirtualTable with the joined tables.

			Table[] tabs = new Table[] { this, table };
			IntegerVector[] row_sets = new IntegerVector[] { this_row_set, table_row_set };

			VirtualTable out_table = new VirtualTable(tabs);
			out_table.Set(tabs, row_sets);

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								out_table + " = " + this + ".SimpleJoin(" + table +
								", " + lhs_var + ", " + op + ", " + rhs + " )");
				}
			}

			return out_table;

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
			int row_count = RowCount;
			if (row_count > 0) {

				TableVariableResolver resolver = GetVariableResolver();
				IRowEnumerator e = GetRowEnumerator();

				IntegerVector selected_set = new IntegerVector(row_count);

				while (e.MoveNext()) {
					int row_index = e.RowIndex;
					resolver.SetId = row_index;

					// Resolve expression into a constant.
					TObject rhs_val = exp.Evaluate(resolver, context);

					// If resolved to true then include in the selected set.
					if (!rhs_val.IsNull && rhs_val.TType is TBooleanType &&
						rhs_val.Object.Equals(true)) {
						selected_set.AddInt(row_index);
					}

				}

				// Make into a table to return.
				VirtualTable table = new VirtualTable(this);
				table.Set(this, selected_set);

				result = table;
			}

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								result + " = " + this + ".ExhaustiveSelect(" +
								exp + " )");
				}
			}

			return result;
		}

		/// <summary>
		/// Evaluates a non-correlated ANY type operator given the LHS 
		/// expression, the RHS subquery and the ANY operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="lhs">The left has side expression. The <see cref="VariableName"/>
		/// objects in this expression must all reference columns in this table.</param>
		/// <param name="op">The operator to use.</param>
		/// <param name="right_table">The subquery table should only contain 
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
		public virtual Table Any(IQueryContext context, Expression lhs, Operator op, Table right_table) {
			Table table = right_table;

			// Check the table only has 1 column
			if (table.ColumnCount != 1) {
				throw new ApplicationException("Input table <> 1 columns.");
			}

			// Handle trivial case of no entries to select from
			if (RowCount == 0) {
				return this;
			}
			// If 'table' is empty then we return an empty set.  ANY { empty set } is
			// always false.
			if (table.RowCount == 0) {
				return EmptySelect();
			}

			// Is the lhs expression a constant?
			if (lhs.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject lhs_const = lhs.Evaluate(null, context);
				// Select from the table.
				IntegerVector ivec = table.SelectRows(0, op, lhs_const);
				if (ivec.Count > 0) {
					// There's some entries so return the whole table,
					return this;
				}
				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table source_table;
			int lhs_col_index;
			// Is the lhs expression a single variable?
			VariableName lhs_var = lhs.VariableName;
			// NOTE: It'll be less common for this part to be called.
			if (lhs_var == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				FunctionTable fun_table = new FunctionTable(
					  this, new Expression[] { lhs }, new String[] { "1" }, db_context);
				source_table = fun_table;
				lhs_col_index = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				source_table = this;
				lhs_col_index = source_table.FindFieldName(lhs_var);
				if (lhs_col_index == -1) {
					throw new ApplicationException("Can't find column '" + lhs_var + "'.");
				}
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataTableColumnDef source_col = source_table.GetColumnDef(lhs_col_index);
			DataTableColumnDef dest_col = table.GetColumnDef(0);
			if (!source_col.TType.IsComparableType(dest_col.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
								source_col.SQLTypeString + " is incompatible " +
								"with the sub-query " + dest_col.SQLTypeString +
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

			IntegerVector select_vec;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the first from the set (the lowest value),
				TObject lowest_cell = table.GetFirstCellContent(0);
				// Select from the source table all rows that are > or >= to the
				// lowest cell,
				select_vec = source_table.SelectRows(lhs_col_index, op, lowest_cell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the last from the set (the highest value),
				TObject highest_cell = table.GetLastCellContent(0);
				// Select from the source table all rows that are < or <= to the
				// highest cell,
				select_vec = source_table.SelectRows(lhs_col_index, op, highest_cell);
			} else if (op.IsEquivalent("=")) {
				// Equiv. to IN
				select_vec = INHelper.In(source_table, table, lhs_col_index, 0);
			} else if (op.IsEquivalent("<>")) {
				// Select the value that is the same of the entire column
				TObject cell = table.GetSingleCellContent(0);
				if (cell != null) {
					// All values from 'source_table' that are <> than the given cell.
					select_vec = source_table.SelectRows(lhs_col_index, op, cell);
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
			rtable.Set(this, select_vec);

			// Query logging information
			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this,
							rtable + " = " + this + ".any(" +
							lhs + ", " + op + ", " + right_table + ")");
			}

			return rtable;
		}

		/// <summary>
		/// Evaluates a non-correlated ALL type operator given the LHS expression,
		/// the RHS subquery and the ALL operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="lhs">Expression containing <see cref="VariableName"/> 
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
		public virtual Table All(IQueryContext context, Expression lhs, Operator op, Table table) {

			// Check the table only has 1 column
			if (table.ColumnCount != 1) {
				throw new ApplicationException("Input table <> 1 columns.");
			}

			// Handle trivial case of no entries to select from
			if (RowCount == 0) {
				return this;
			}
			// If 'table' is empty then we return the complete set.  ALL { empty set }
			// is always true.
			if (table.RowCount == 0) {
				return this;
			}

			// Is the lhs expression a constant?
			if (lhs.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject lhs_const = lhs.Evaluate(null, context);
				bool compared_to_true;
				// The various operators
				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Find the maximum value in the table
					TObject cell = table.GetLastCellContent(0);
					compared_to_true = CompareCells(lhs_const, cell, op);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Find the minimum value in the table
					TObject cell = table.GetFirstCellContent(0);
					compared_to_true = CompareCells(lhs_const, cell, op);
				} else if (op.IsEquivalent("=")) {
					// Only true if rhs is a single value
					TObject cell = table.GetSingleCellContent(0);
					compared_to_true = (cell != null && CompareCells(lhs_const, cell, op));
				} else if (op.IsEquivalent("<>")) {
					// true only if lhs_cell is not found in column.
					compared_to_true = !table.ColumnContainsCell(0, lhs_const);
				} else {
					throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
				}

				// If matched return this table
				if (compared_to_true) {
					return this;
				}
				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table source_table;
			int lhs_col_index;
			// Is the lhs expression a single variable?
			VariableName lhs_var = lhs.VariableName;
			// NOTE: It'll be less common for this part to be called.
			if (lhs_var == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				FunctionTable fun_table = new FunctionTable(
					  this, new Expression[] { lhs }, new String[] { "1" }, db_context);
				source_table = fun_table;
				lhs_col_index = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				source_table = this;
				lhs_col_index = source_table.FindFieldName(lhs_var);
				if (lhs_col_index == -1) {
					throw new ApplicationException("Can't find column '" + lhs_var + "'.");
				}
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataTableColumnDef source_col = source_table.GetColumnDef(lhs_col_index);
			DataTableColumnDef dest_col = table.GetColumnDef(0);
			if (!source_col.TType.IsComparableType(dest_col.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
								source_col.SQLTypeString + " is incompatible " +
								"with the sub-query " + dest_col.SQLTypeString +
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

			IntegerVector select_vec;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the last from the set (the highest value),
				TObject highest_cell = table.GetLastCellContent(0);
				// Select from the source table all rows that are > or >= to the
				// highest cell,
				select_vec = source_table.SelectRows(lhs_col_index, op, highest_cell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the first from the set (the lowest value),
				TObject lowest_cell = table.GetFirstCellContent(0);
				// Select from the source table all rows that are < or <= to the
				// lowest cell,
				select_vec = source_table.SelectRows(lhs_col_index, op, lowest_cell);
			} else if (op.IsEquivalent("=")) {
				// Select the single value from the set (if there is one).
				TObject single_cell = table.GetSingleCellContent(0);
				if (single_cell != null) {
					// Select all from source_table all values that = this cell
					select_vec = source_table.SelectRows(lhs_col_index, op, single_cell);
				} else {
					// No single value so return empty set (no value in LHS will equal
					// a value in RHS).
					return EmptySelect();
				}
			} else if (op.IsEquivalent("<>")) {
				// Equiv. to NOT IN
				select_vec = INHelper.NotIn(source_table, table, lhs_col_index, 0);
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, select_vec);

			// Query logging information
			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this,
							rtable + " = " + this + ".all(" +
							lhs + ", " + op + ", " + table + ")");
			}

			return rtable;
		}



		// ---------- The original table functions ----------

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table, bool quick) {
			Table out_table;

			if (quick) {
				// This implementation doesn't materialize the join
				out_table = new NaturallyJoinedTable(this, table);
			} else {

				Table[] tabs = new Table[2];
				tabs[0] = this;
				tabs[1] = table;
				IntegerVector[] row_sets = new IntegerVector[2];

				// Optimized trivial case, if either table has zero rows then result of
				// join will contain zero rows also.
				if (RowCount == 0 || table.RowCount == 0) {

					row_sets[0] = new IntegerVector(0);
					row_sets[1] = new IntegerVector(0);

				} else {

					// The natural join algorithm.
					IntegerVector this_row_set = new IntegerVector();
					IntegerVector table_row_set = new IntegerVector();

					// Get the set of all rows in the given table.
					IntegerVector table_selected_set = new IntegerVector();
					IRowEnumerator e = table.GetRowEnumerator();
					while (e.MoveNext()) {
						int row_index = e.RowIndex;
						table_selected_set.AddInt(row_index);
					}
					int table_selected_set_size = table_selected_set.Count;

					// Join with the set of rows in this table.
					e = GetRowEnumerator();
					while (e.MoveNext()) {
						int row_index = e.RowIndex;
						for (int i = 0; i < table_selected_set_size; ++i) {
							this_row_set.AddInt(row_index);
						}
						table_row_set.Append(table_selected_set);
					}

					// The row sets we are joining from each table.
					row_sets[0] = this_row_set;
					row_sets[1] = table_row_set;
				}

				// Create the new VirtualTable with the joined tables.
				VirtualTable virt_table = new VirtualTable(tabs);
				virt_table.Set(tabs, row_sets);

				out_table = virt_table;

			}

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								out_table + " = " + this + ".naturalJoin(" + table + " )");
				}
			}

			return out_table;
		}

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table) {
			return Join(table, true);
		}

		/// <summary>
		/// Finds all rows in this table that are <i>outside</i> the result
		/// in the given table.
		/// </summary>
		/// <param name="rtable">The right table that must be a decendent of 
		/// this table.</param>
		/// <remarks>
		/// Performs a normal join, then determines unmatched joins.
		/// <para>
		/// It is possible to create an OuterTable with this result to make 
		/// the completed table.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public VirtualTable Outside(Table rtable) {

			// Form the row list for right hand table,
			IntegerVector row_list = new IntegerVector(rtable.RowCount);
			IRowEnumerator e = rtable.GetRowEnumerator();
			while (e.MoveNext()) {
				row_list.AddInt(e.RowIndex);
			}
			int col_index = rtable.FindFieldName(GetResolvedVariable(0));
			rtable.SetToRowTableDomain(col_index, row_list, this);

			// This row set
			IntegerVector this_table_set = new IntegerVector(RowCount);
			e = GetRowEnumerator();
			while (e.MoveNext()) {
				this_table_set.AddInt(e.RowIndex);
			}

			// 'row_list' is now the rows in this table that are in 'rtable'.
			// Sort both 'this_table_set' and 'row_list'
			this_table_set.QuickSort();
			row_list.QuickSort();

			// Find all rows that are in 'this_table_set' and not in 'row_list'
			IntegerVector result_list = new IntegerVector(96);
			int size = this_table_set.Count;
			int row_list_index = 0;
			int row_list_size = row_list.Count;
			for (int i = 0; i < size; ++i) {
				int this_val = this_table_set[i];
				if (row_list_index < row_list_size) {
					int in_val = row_list[row_list_index];
					if (this_val < in_val) {
						result_list.AddInt(this_val);
					} else if (this_val == in_val) {
						while (row_list_index < row_list_size &&
							   row_list[row_list_index] == in_val) {
							++row_list_index;
						}
					} else {
						throw new ApplicationException("'this_val' > 'in_val'");
					}
				} else {
					result_list.AddInt(this_val);
				}
			}

			// Return the new VirtualTable
			VirtualTable table = new VirtualTable(this);
			table.Set(this, result_list);

			return table;
		}

		/// <summary>
		/// Returns a new Table that is the union of the this table and 
		/// the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// A union operation will remove any duplicate rows.
		/// </remarks>
		/// <returns></returns>
		public Table Union(Table table) {

			// Optimizations - handle trivial case of row count in one of the tables
			//   being 0.
			// NOTE: This optimization assumes this table and the unioned table are
			//   of the same type.
			if ((RowCount == 0 && table.RowCount == 0) ||
				 table.RowCount == 0) {

				if (DEBUG_QUERY) {
					if (Debug.IsInterestedIn(DebugLevel.Information)) {
						Debug.Write(DebugLevel.Information, this, this + " = " + this + ".Union(" + table + " )");
					}
				}
				return this;
			} 
			if (RowCount == 0) {
				if (DEBUG_QUERY) {
					if (Debug.IsInterestedIn(DebugLevel.Information)) {
						Debug.Write(DebugLevel.Information, this, table + " = " + this + ".Union(" + table + " )");
					}
				}
				return table;
			}

			// First we merge this table with the input table.

			RawTableInformation raw1 = ResolveToRawTable(new RawTableInformation());
			RawTableInformation raw2 = table.ResolveToRawTable(new RawTableInformation());

			// This will throw an exception if the table types do not match up.

			raw1.Union(raw2);

			// Now 'raw1' contains a list of uniquely merged rows (ie. the union).
			// Now make it into a new table and return the information.

			Table[] table_list = raw1.GetTables();
			VirtualTable table_out = new VirtualTable(table_list);
			table_out.Set(table_list, raw1.GetRows());

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								table_out + " = " + this + ".Union(" + table + " )");
				}
			}

			return table_out;
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

			Table[] table_list = raw.GetTables();
			VirtualTable table_out = new VirtualTable(table_list);
			table_out.Set(table_list, raw.GetRows());

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								table_out + " = " + this + ".distinct()");
				}
			}

			return table_out;
		}

		/// <summary>
		/// Returns a new table that has only distinct rows in it.
		/// </summary>
		/// <param name="col_map">Integer array containing the columns 
		/// to make distinct over.</param>
		/// <remarks>
		/// This is an expensive operation. We sort over all the columns, then 
		/// iterate through the result taking out any duplicate rows.
		/// <para>
		/// <b>Note</b>: This will change the order of this table in the result.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table Distinct(int[] col_map) {
			IntegerVector result_list = new IntegerVector();
			IntegerVector row_list = OrderedRowList(col_map);

			int r_count = row_list.Count;
			int previous_row = -1;
			for (int i = 0; i < r_count; ++i) {
				int row_index = row_list[i];

				if (previous_row != -1) {

					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < col_map.Length && equal; ++n) {
						TObject c1 = GetCellContents(col_map[n], row_index);
						TObject c2 = GetCellContents(col_map[n], previous_row);
						equal = equal && (c1.CompareTo(c2) == 0);
					}

					if (!equal) {
						result_list.AddInt(row_index);
					}
				} else {
					result_list.AddInt(row_index);
				}

				previous_row = row_index;
			}

			// Return the new table with distinct rows only.
			VirtualTable vt = new VirtualTable(this);
			vt.Set(this, result_list);

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								vt + " = " + this + ".distinct(" + col_map + ")");
				}
			}

			return vt;

		}


		/// <summary>
		/// Helper function.  Returns the index in the String array of the given
		/// string value.
		/// </summary>
		/// <param name="val"></param>
		/// <param name="array"></param>
		/// <returns></returns>
		private int IndexStringArray(String val, String[] array) {
			for (int n = 0; n < array.Length; ++n) {
				if (array[n].Equals(val)) {
					return n;
				}
			}
			return -1;
		}


		/// <summary>
		/// Returns true if the given column number contains the value given.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool ColumnContainsValue(int column, TObject ob) {
			return ColumnMatchesValue(column, Operator.Get("="), ob);
		}

		/// <summary>
		/// Returns true if the given column contains a value that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool ColumnMatchesValue(int column, Operator op, TObject ob) {
			IntegerVector ivec = SelectRows(column, op, ob);
			return (ivec.Count > 0);
		}

		/// <summary>
		/// Returns true if the given column contains all values that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool AllColumnMatchesValue(int column, Operator op, TObject ob) {
			IntegerVector ivec = SelectRows(column, op, ob);
			return (ivec.Count == RowCount);
		}

		/// <summary>
		/// Order the table by the given columns.
		/// </summary>
		/// <param name="col_map">Column indices to order by the table.</param>
		/// <returns>
		/// Returns a table that is ordered by the given column numbers.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public Table OrderByColumns(int[] col_map) {
			// Sort by the column list.
			Table work = this;
			for (int i = col_map.Length - 1; i >= 0; --i) {
				work = work.OrderByColumn(col_map[i], true);
			}
			// A nice post condition to check on.
			if (RowCount != work.RowCount) {
				throw new ApplicationException("Internal Error, row count != sorted row count");
			}

			return work;
		}

		/// <summary>
		/// Gets an ordered list of rows.
		/// </summary>
		/// <param name="col_map">Column indices to order by the rows.</param>
		/// <returns>
		/// Returns an <see cref="IntegerVector"/> that represents the list of 
		/// rows in this table in sorted order by the given <paramref name="col_map"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public IntegerVector OrderedRowList(int[] col_map) {
			Table work = OrderByColumns(col_map);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			int r_count = RowCount;
			IntegerVector row_list = new IntegerVector(r_count);
			IRowEnumerator e = work.GetRowEnumerator();
			while (e.MoveNext()) {
				row_list.AddInt(e.RowIndex);
			}

			work.SetToRowTableDomain(0, row_list, this);
			return row_list;
		}


		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="col_index"/>.
		/// </summary>
		/// <param name="col_index">Index of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="col_index"/>.
		/// </returns>
		public VirtualTable OrderByColumn(int col_index, bool ascending) {
			// Check the field can be sorted
			DataTableColumnDef col_def = GetColumnDef(col_index);

			IntegerVector rows = SelectAll(col_index);

			// Reverse the list if we are not ascending
			if (ascending == false) {
				rows.Reverse();
			}

			// We now has an int[] array of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								table + " = " + this + ".OrderByColumn(" +
								col_index + ", " + ascending + ")");
				}
			}

			return table;
		}

		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="column"/>.
		/// </summary>
		/// <param name="column">Name of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="column"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the given column name was not found.
		/// </exception>
		public VirtualTable OrderByColumn(VariableName column, bool ascending) {
			int col_index = FindFieldName(column);
			if (col_index == -1) {
				throw new ApplicationException("Unknown column in 'OrderByColumn' ( " + column + " )");
			}
			return OrderByColumn(col_index, ascending);
		}

		public VirtualTable OrderByColumn(VariableName column) {
			return OrderByColumn(column, true);
		}


		/// <summary>
		/// Gets an object that can only access the cells that are in this
		/// table, and has no other access to the <see cref="Table"/> 
		/// functionalities.
		/// </summary>
		/// <remarks>
		/// The purpose of this object is to provide a clean way to access the state 
		/// of a table without being able to access any of the row sorting
		/// (SelectableScheme) methods that would return incorrect information in the
		/// situation where the table locks (via LockingMechanism) were removed.
		/// <para>
		/// <b>Note:</b> The methods in this class will only work if this table has 
		/// its rows locked via the <see cref="LockRoot"/> method.
		/// </para>
		/// </remarks>
		public TableAccessState GetTableAccessState() {
			return new TableAccessState(this);
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
		internal IntegerVector SelectRows(int[] cols, Operator op, TObject[] cells) {
			// PENDING: Look for an multi-column index to make this a lot faster,
			if (cols.Length > 1) {
				throw new ApplicationException("Multi-column select not supported.");
			}
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
		internal IntegerVector SelectRows(int column, Operator op, TObject cell) {
			// If the cell is of an incompatible type, return no results,
			TType col_type = GetTTypeForColumn(column);
			if (!cell.TType.IsComparableType(col_type)) {
				// Types not comparable, so return 0
				return new IntegerVector(0);
			}

			// Get the selectable scheme for this column
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);

			// If the operator is a standard operator, use the interned SelectableScheme
			// methods.
			if (op.IsEquivalent("=")) {
				return ss.SelectEqual(cell);
			} else if (op.IsEquivalent("<>")) {
				return ss.SelectNotEqual(cell);
			} else if (op.IsEquivalent(">")) {
				return ss.SelectGreater(cell);
			} else if (op.IsEquivalent("<")) {
				return ss.SelectLess(cell);
			} else if (op.IsEquivalent(">=")) {
				return ss.SelectGreaterOrEqual(cell);
			} else if (op.IsEquivalent("<=")) {
				return ss.SelectLessOrEqual(cell);
			}

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			SelectableRangeSet range_set = new SelectableRangeSet();
			range_set.Intersect(op, cell);
			return ss.SelectRange(range_set.ToArray());
		}

		/// <summary>
		/// Selects the rows in a table column between two minimum and maximum 
		/// bounds.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="min_cell"></param>
		/// <param name="max_cell"></param>
		/// <remarks>
		/// <b>Note</b> The returns IntegerList <b>must</b> be sorted be the 
		/// <paramref name="column"/> cells.
		/// </remarks>
		/// <returns>
		/// Returns all the rows in the table with the value of <paramref name="column"/>
		/// column greater or equal then <paramref name="min_cell"/> and smaller then
		/// <paramref name="max_cell"/>.
		/// </returns>
		internal virtual IntegerVector SelectRows(int column, TObject min_cell, TObject max_cell) {
			// Check all the tables are comparable
			TType col_type = GetTTypeForColumn(column);
			if (!min_cell.TType.IsComparableType(col_type) ||
				!max_cell.TType.IsComparableType(col_type)) {
				// Types not comparable, so return 0
				return new IntegerVector(0);
			}

			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectBetween(min_cell, max_cell);
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
		internal IntegerVector SelectFromRegex(int column, Operator op, TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}

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
		internal virtual IntegerVector SelectFromPattern(int column, Operator op, TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector();
			}

			if (op.IsEquivalent("not like")) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.
				IntegerVector like_set =
									  PatternSearch.Search(this, column, ob.ToString());
				// Don't include NULL values
				TObject null_cell = new TObject(ob.TType, null);
				IntegerVector original_set =
								  SelectRows(column, Operator.Get("is not"), null_cell);
				int vec_size = SysMath.Max(4, (original_set.Count - like_set.Count) + 4);
				IntegerVector result_set = new IntegerVector(vec_size);
				like_set.QuickSort();
				int size = original_set.Count;
				for (int i = 0; i < size; ++i) {
					int val = original_set[i];
					// If val not in like set, add to result
					if (like_set.SortedIntCount(val) == 0) {
						result_set.AddInt(val);
					}
				}
				return result_set;
			} else { // if (op.is("like")) {
				return PatternSearch.Search(this, column, ob.ToString());
			}
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are also in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		internal virtual IntegerVector AllRowsIn(int column, Table table) {
			IntegerVector iv = INHelper.In(this, table, column, 0);
			return iv;
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are not in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		internal virtual IntegerVector AllRowsNotIn(int column, Table table) {
			return INHelper.NotIn(this, table, column, 0);
		}

		/// <summary>
		/// Returns an array that represents the sorted order of this table by
		/// the given column number.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IntegerVector SelectAll(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectAll();
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
		/// <param name="column"></param>
		/// <param name="ranges"></param>
		/// <remarks>
		/// If there is an index on the column, the result can be found very quickly.
		/// The range array must be normalized (no overlapping ranges).
		/// </remarks>
		/// <returns></returns>
		public IntegerVector SelectRange(int column, SelectableRange[] ranges) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectRange(ranges);
		}

		/// <summary>
		/// Returns a list that represents the last sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IntegerVector SelectLast(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectLast();
		}

		/// <summary>
		/// Returns a list that represents the first sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IntegerVector SelectFirst(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectFirst();
		}

		/// <summary>
		/// Returns a list that represents the rest of the sorted element(s) of the
		/// given column index (not the <i>first</i> set).
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IntegerVector SelectRest(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectNotFirst();
		}

		/// <summary>
		/// Convenience, returns a TObject[] array given a single TObject, or
		/// null if the TObject is null (not if TObject represents a null value).
		/// </summary>
		/// <param name="cell"></param>
		/// <returns></returns>
		private static TObject[] SingleArrayCellMap(TObject cell) {
			return cell == null ? null : new TObject[] { cell };
		}

		/// <summary>
		/// Gets the first value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> value that represents the first item 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject GetFirstCellContent(int column) {
			IntegerVector ivec = SelectFirst(column);
			if (ivec.Count > 0) {
				return GetCellContents(column, ivec[0]);
			}
			return null;
		}

		/// <summary>
		/// Gets an array of the first values of the given columns.
		/// </summary>
		/// <param name="col_map"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> values that represents the first items 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject[] GetFirstCellContent(int[] col_map) {
			if (col_map.Length > 1) {
				throw new ApplicationException("Multi-column GetLastCellContent not supported.");
			}
			return SingleArrayCellMap(GetFirstCellContent(col_map[0]));
		}

		/// <summary>
		/// Gets the last value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		/// </returns>
		public TObject GetLastCellContent(int column) {
			IntegerVector ivec = SelectLast(column);
			if (ivec.Count > 0) {
				return GetCellContents(column, ivec[0]);
			}
			return null;
		}

		///<summary>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		///</summary>
		///<param name="col_map"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetLastCellContent(int[] col_map) {
			if (col_map.Length > 1) {
				throw new ApplicationException("Multi-column GetLastCellContent not supported.");
			}
			return SingleArrayCellMap(GetLastCellContent(col_map[0]));
		}

		/// <summary>
		/// If the given column contains all items of the same value, this method
		/// returns the value.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the value of the column if all its the cells contains the
		/// same value, otherwise returns <b>null</b>.
		/// </returns>
		public TObject GetSingleCellContent(int column) {
			IntegerVector ivec = SelectFirst(column);
			int sz = ivec.Count;
			if (sz == RowCount && sz > 0) {
				return GetCellContents(column, ivec[0]);
			}
			return null;
		}

		///<summary>
		/// If the given column contains all items of the same value, this 
		/// method returns the value.
		///</summary>
		///<param name="col_map"></param>
		///<returns></returns>
		/// <remarks>
		/// If it doesn't, or the column set is empty it returns null.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetSingleCellContent(int[] col_map) {
			if (col_map.Length > 1) {
				throw new ApplicationException("Multi-column GetSingleCellContent not supported.");
			}
			return SingleArrayCellMap(GetSingleCellContent(col_map[0]));
		}

		/// <summary>
		/// Checks if the given column contains the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="cell"></param>
		/// <returns>
		/// Returns <b>true</b> if the given value is found in the table
		/// for the given column, otherwise <b>false</b>.
		/// </returns>
		public bool ColumnContainsCell(int column, TObject cell) {
			IntegerVector ivec = SelectRows(column, Operator.Get("="), cell);
			return ivec.Count > 0;
		}

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
		public static bool CompareCells(TObject ob1, TObject ob2, Operator op) {
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
		/// Converts the table to a <see cref="IDictionary"/>.
		/// </summary>
		/// <returns>
		/// Returns the table as a <see cref="IDictionary"/>
		/// with the key/pair set.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the table has more or less then two columns or if the first 
		/// column is not a string column.
		/// </exception>
		public IDictionary ToDictionary() {
			if (ColumnCount == 2) {
				Hashtable map = new Hashtable();
				IRowEnumerator en = GetRowEnumerator();
				while (en.MoveNext()) {
					int row_index = en.RowIndex;
					TObject key = GetCellContents(0, row_index);
					TObject value = GetCellContents(1, row_index);
					map[key.Object.ToString()] = value.Object;
				}
				return map;
			} else {
				throw new ApplicationException("Table must have two columns.");
			}
		}







		// Stores col name -> col index lookups
		private Hashtable col_name_lookup;
		private Object COL_LOOKUP_LOCK = new Object();

		/// <summary>
		/// Provides faster way to find a column index given a column name.
		/// </summary>
		/// <param name="col">Name of the column to get the index for.</param>
		/// <returns>
		/// Returns the index of the column for the given name, or -1
		/// if not found.
		/// </returns>
		public int FastFindFieldName(VariableName col) {
			lock (COL_LOOKUP_LOCK) {
				if (col_name_lookup == null) {
					col_name_lookup = new Hashtable(30);
				}
				Object ob = col_name_lookup[col];
				if (ob == null) {
					int ci = FindFieldName(col);
					col_name_lookup[col] = ci;
					return ci;
				} else {
					return (int)ob;
				}
			}
		}

		/// <summary>
		/// Returns a TableVariableResolver object for this table.
		/// </summary>
		/// <returns></returns>
		internal TableVariableResolver GetVariableResolver() {
			return new TableVariableResolver(this);
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// An implementation of <see cref="IVariableResolver"/> that we can use 
		/// to resolve column names in this table to cells for a specific row.
		/// </summary>
		internal class TableVariableResolver : IVariableResolver {
			public TableVariableResolver(Table table) {
				this.table = table;
			}

			private readonly Table table;
			private int row_index = -1;

			private int FindColumnName(VariableName variable) {
				int col_index = table.FastFindFieldName(variable);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + variable);
				}
				return col_index;
			}

			// --- Implemented ---

			public int SetId {
				get { return row_index; }
				set { row_index = value; }
			}

			public TObject Resolve(VariableName variable) {
				return table.GetCellContents(FindColumnName(variable), row_index);
			}

			public TType ReturnTType(VariableName variable) {
				return table.GetTTypeForColumn(variable);
			}

		}

		/// <inheritdoc/>
		public override String ToString() {
			String name = "VT" + GetHashCode();
			if (this is DataTableBase) {
				name = ((DataTableBase)this).TableName.ToString();
			}
			return name + "[" + RowCount + "]";
		}

		/// <summary>
		/// Prints a graph of the table hierarchy to the stream.
		/// </summary>
		/// <param name="output"></param>
		/// <param name="indent"></param>
		public virtual void PrintGraph(TextWriter output, int indent) {
			for (int i = 0; i < indent; ++i) {
				output.Write(' ');
			}
			output.WriteLine("T[" + GetType() + "]");
		}
	}
}