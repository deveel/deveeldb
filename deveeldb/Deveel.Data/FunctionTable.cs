//  
//  FunctionTable.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using Deveel.Data.Caching;
using Deveel.Data.Collections;
using Deveel.Diagnostics;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// A table that has a number of columns and as many rows as the 
	/// refering table.
	/// </summary>
	/// <remarks>
	/// Tables of this type are used to construct aggregate and function
	/// columns based on an expression.
	/// They are joined with the result table in the last part of the query 
	/// processing.
	/// <para>
	/// For example, a query like <c>SELECT id, id * 2, 8 * 9 FROM Part</c> the
	/// columns <c>id * 2</c> and <c>8 * 9</c> would be formed from this table.
	/// </para>
	/// <para>
	/// <b>Synchronization Issue:</b> Instances of this object are <b>not</b> 
	/// thread safe. The reason it's not is because if <see cref="GetCellContents"/> 
	/// is used concurrently it's possible for the same value to be added into 
	/// the cache causing an error.
	/// It is not expected that this object will be shared between threads.
	/// </para>
	/// </remarks>
	public class FunctionTable : DefaultDataTable {
		/// <summary>
		/// The table name given to all function tables.
		/// </summary>
		private static readonly TableName FunctionTableName = new TableName(null, "FUNCTIONTABLE");

		/// <summary>
		/// The key used to make distinct unique ids for FunctionTables.
		///</summary>
		/// <remarks>
		/// <b>Note</b>: This is a thread-safe static mutable variable.
		/// </remarks>
		private static int UniqueKeySeq = 0;

		/// <summary>
		/// The context of this function table.
		/// </summary>
		private readonly IQueryContext context;

		/// <summary>
		/// The TableVariableResolver for the table we are cross referencing.
		/// </summary>
		private readonly TableVariableResolver cr_resolver;

		private readonly Table cross_ref_table;

		/// <summary>
		/// Some information about the expression list.  If the value is 0 then the
		/// column is simple to solve and shouldn't be cached.
		/// </summary>
		private readonly byte[] exp_info;

		/// <summary>
		/// The list of expressions that are evaluated to form each column.
		/// </summary>
		private readonly Expression[] exp_list;

		/// <summary>
		/// The DataTableInfo object that describes the columns in this function
		/// table.
		/// </summary>
		private readonly DataTableDef fun_table_def;

		/// <summary>
		/// A unique id given to this FunctionTable when it is created.  No two
		/// FunctionTable objects may have the same number.  This number is between
		/// 0 and 260 million.
		/// </summary>
		private readonly int unique_id;

		/// <summary>
		/// The group row links.
		/// </summary>
		/// <remarks>
		/// Iterate through this to find all the rows in a group until bit 31 set.
		/// </remarks>
		private IntegerVector group_links;

		/// <summary>
		/// The lookup mapping for row->group_index used for grouping.
		/// </summary>
		private IntegerVector group_lookup;

		/// <summary>
		/// The TableGroupResolver for the table.
		/// </summary>
		private TableGroupResolver group_resolver;

		/// <summary>
		/// Whether the whole table is a group.
		/// </summary>
		private bool whole_table_as_group;

		/// <summary>
		/// If the whole table is a group, this is the grouping rows.
		/// </summary>
		/// <remarks>
		/// This is obtained via <see cref="Table.SelectAll()"/> of the reference table.
		/// </remarks>
		private IntegerVector whole_table_group;

		/// <summary>
		/// The total size of the whole table group size.
		/// </summary>
		private int whole_table_group_size;

		/// <summary>
		/// If the whole table is a simple enumeration (row index is 0 to 
		/// <see cref="Table.RowCount"/>) then this is true.
		/// </summary>
		private bool whole_table_is_simple_enum;


		///<summary>
		///</summary>
		///<param name="cross_ref_table"></param>
		///<param name="in_exp_list"></param>
		///<param name="col_names"></param>
		///<param name="context"></param>
		public FunctionTable(Table cross_ref_table, Expression[] in_exp_list,
		                     String[] col_names, DatabaseQueryContext context)
			: base(context.Database) {
			// Make sure we are synchronized over the class.
			lock (typeof (FunctionTable)) {
				unique_id = UniqueKeySeq;
				++UniqueKeySeq;
			}
			unique_id = (unique_id & 0x0FFFFFFF) | 0x010000000;

			this.context = context;

			this.cross_ref_table = cross_ref_table;
			cr_resolver = cross_ref_table.GetVariableResolver();
			cr_resolver.SetId = 0;

			// Create a DataTableDef object for this function table.
			fun_table_def = new DataTableDef();
			fun_table_def.TableName = FunctionTableName;

			exp_list = new Expression[in_exp_list.Length];
			exp_info = new byte[in_exp_list.Length];

			// Create a new DataTableColumnDef for each expression, and work out if the
			// expression is simple or not.
			for (int i = 0; i < in_exp_list.Length; ++i) {
				Expression expr = in_exp_list[i];
				// Examine the expression and determine if it is simple or not
				if (expr.IsConstant && !expr.HasAggregateFunction(context)) {
					// If expression is a constant, solve it
					TObject result = expr.Evaluate(null, null, context);
					expr = new Expression(result);
					exp_list[i] = expr;
					exp_info[i] = 1;
				} else {
					// Otherwise must be dynamic
					exp_list[i] = expr;
					exp_info[i] = 0;
				}
				// Make the column def
				DataTableColumnDef column = new DataTableColumnDef();
				column.Name = col_names[i];
				column.SetFromTType(expr.ReturnTType(cr_resolver, context));
				fun_table_def.AddVirtualColumn(column);
			}

			// Make sure the table def isn't changed from this point on.
			fun_table_def.SetImmutable();

			// Function tables are the size of the referring table.
			row_count = cross_ref_table.RowCount;

			// Set schemes to 'blind search'.
			BlankSelectableSchemes(1);
		}

		///<summary>
		///</summary>
		///<param name="exp_list"></param>
		///<param name="col_names"></param>
		///<param name="context"></param>
		public FunctionTable(Expression[] exp_list, String[] col_names,
		                     DatabaseQueryContext context)
			: this(context.Database.SingleRowTable,
			       exp_list, col_names, context) {
		}

		///<summary>
		/// Returns the Table this function is based on.
		///</summary>
		/// <remarks>
		/// We need to provide this method for aggregate functions.
		/// </remarks>
		public Table ReferenceTable {
			get { return cross_ref_table; }
		}

		public override DataTableDef DataTableDef {
			get { return fun_table_def; }
		}

		public override bool HasRootsLocked {
			get { return ReferenceTable.HasRootsLocked; }
		}

		/// <summary>
		/// Return a TObject that represents the value of the 'column', 'row' of
		/// this table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <param name="cache"></param>
		/// <remarks>
		/// If 'cache' is not null then the resultant value is added to
		/// the cache.  If 'cache' is null, no caching happens.
		/// </remarks>
		/// <returns></returns>
		private TObject CalcValue(int column, int row, DataCellCache cache) {
			cr_resolver.SetId = row;
			if (group_resolver != null) {
				group_resolver.SetUpGroupForRow(row);
			}
			Expression expr = exp_list[column];
			TObject cell = expr.Evaluate(group_resolver, cr_resolver, context);
			if (cache != null) {
				cache.Set(unique_id, row, column, cell);
			}
			return cell;
		}

		// ------ Public methods ------

		///<summary>
		/// Sets the whole reference table as a single group.
		///</summary>
		public void SetWholeTableAsGroup() {
			whole_table_as_group = true;

			whole_table_group_size = ReferenceTable.RowCount;

			// Set up 'whole_table_group' to the list of all rows in the reference
			// table.
			IRowEnumerator en = ReferenceTable.GetRowEnumerator();
			whole_table_is_simple_enum = en is SimpleRowEnumerator;
			if (!whole_table_is_simple_enum) {
				whole_table_group = new IntegerVector(ReferenceTable.RowCount);
				while (en.MoveNext()) {
					whole_table_group.AddInt(en.RowIndex);
				}
			}

			// Set up a group resolver for this method.
			group_resolver = new TableGroupResolver(this);
		}

		/// <summary>
		/// Creates a grouping matrix for the given columns.
		/// </summary>
		/// <param name="col_list"></param>
		/// <remarks>
		/// The grouping matrix is arranged so that each row of the refering 
		/// table that is in the group is given a number that refers to the top 
		/// group entry in the group list. The group list is a linked integer 
		/// list that chains through each row item in the list.
		/// </remarks>
		public void CreateGroupMatrix(Variable[] col_list) {
			// If we have zero rows, then don't bother creating the matrix.
			if (RowCount <= 0 || col_list.Length <= 0) {
				return;
			}

			Table root_table = ReferenceTable;
			int r_count = root_table.RowCount;
			int[] col_lookup = new int[col_list.Length];
			for (int i = col_list.Length - 1; i >= 0; --i) {
				col_lookup[i] = root_table.FindFieldName(col_list[i]);
			}
			IntegerVector row_list = root_table.OrderedRowList(col_lookup);

			// 'row_list' now contains rows in this table sorted by the columns to
			// group by.

			// This algorithm will generate two lists.  The group_lookup list maps
			// from rows in this table to the group number the row belongs in.  The
			// group number can be used as an index to the 'group_links' list that
			// contains consequtive links to each row in the group until -1 is reached
			// indicating the end of the group;

			group_lookup = new IntegerVector(r_count);
			group_links = new IntegerVector(r_count);
			int current_group = 0;
			int previous_row = -1;
			for (int i = 0; i < r_count; ++i) {
				int row_index = row_list[i];

				if (previous_row != -1) {
					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < col_lookup.Length && equal; ++n) {
						TObject c1 = root_table.GetCellContents(col_lookup[n], row_index);
						TObject c2 =
							root_table.GetCellContents(col_lookup[n], previous_row);
						equal = equal && (c1.CompareTo(c2) == 0);
					}

					if (!equal) {
						// If end of group, set bit 15
						group_links.AddInt(previous_row | 0x040000000);
						current_group = group_links.Count;
					} else {
						group_links.AddInt(previous_row);
					}
				}

				group_lookup.PlaceIntAt(current_group, row_index); // (val, pos)

				previous_row = row_index;
			}
			// Add the final row.
			group_links.AddInt(previous_row | 0x040000000);

			// Set up a group resolver for this method.
			group_resolver = new TableGroupResolver(this);
		}


		// ------ Methods intended for use by grouping functions ------

		///<summary>
		/// Returns the group of the row at the given index.
		///</summary>
		///<param name="row_index"></param>
		///<returns></returns>
		public int GetRowGroup(int row_index) {
			return group_lookup[row_index];
		}

		///<summary>
		/// The size of the group with the given number.
		///</summary>
		///<param name="group_number"></param>
		///<returns></returns>
		public int GetGroupSize(int group_number) {
			int group_size = 1;
			int i = group_links[group_number];
			while ((i & 0x040000000) == 0) {
				++group_size;
				++group_number;
				i = group_links[group_number];
			}
			return group_size;
		}

		///<summary>
		/// Returns an IntegerVector that represents the list of all rows in 
		/// the group the index is at.
		///</summary>
		///<param name="group_number"></param>
		///<returns></returns>
		public IntegerVector GetGroupRows(int group_number) {
			IntegerVector ivec = new IntegerVector();
			int i = group_links[group_number];
			while ((i & 0x040000000) == 0) {
				ivec.AddInt(i);
				++group_number;
				i = group_links[group_number];
			}
			ivec.AddInt(i & 0x03FFFFFFF);
			return ivec;
		}

		///<summary>
		/// Returns a Table that is this function table merged with the cross
		/// reference table.
		///</summary>
		///<param name="max_column"></param>
		/// <remarks>
		/// The result table includes only one row from each group.
		/// <para>
		/// The 'max_column' argument is optional (can be null).  If it's set to a
		/// column in the reference table, then the row with the max value from the
		/// group is used as the group row.  For example, 'Part.id' will return the
		/// row with the maximum part.id from each group.
		/// </para>
		/// </remarks>
		///<returns></returns>
		public Table MergeWithReference(Variable max_column) {
			Table table = ReferenceTable;

			IntegerVector row_list;

			if (whole_table_as_group) {
				// Whole table is group, so take top entry of table.

				row_list = new IntegerVector(1);
				IRowEnumerator row_enum = table.GetRowEnumerator();
				if (row_enum.MoveNext()) {
					row_list.AddInt(row_enum.RowIndex);
				} else {
					// MAJOR HACK: If the referencing table has no elements then we choose
					//   an arbitary index from the reference table to merge so we have
					//   at least one element in the table.
					//   This is to fix the 'SELECT COUNT(*) FROM empty_table' bug.
					row_list.AddInt(Int32.MaxValue - 1);
				}
			} else if (table.RowCount == 0) {
				row_list = new IntegerVector(0);
			} else if (group_links != null) {
				// If we are grouping, reduce down to only include one row from each
				// group.
				if (max_column == null) {
					row_list = GetTopFromEachGroup();
				} else {
					int col_num = ReferenceTable.FindFieldName(max_column);
					row_list = GetMaxFromEachGroup(col_num);
				}
			} else {
				// OPTIMIZATION: This should be optimized.  It should be fairly trivial
				//   to generate a Table implementation that efficiently merges this
				//   function table with the reference table.

				// This means there is no grouping, so merge with entire table,
				int r_count = table.RowCount;
				row_list = new IntegerVector(r_count);
				IRowEnumerator en = table.GetRowEnumerator();
				while (en.MoveNext()) {
					row_list.AddInt(en.RowIndex);
				}
			}

			// Create a virtual table that's the new group table merged with the
			// functions in this...

			Table[] tabs = new Table[] { table, this };
			IntegerVector[] row_sets = new IntegerVector[] { row_list, row_list };

			VirtualTable out_table = new VirtualTable(tabs);
			out_table.Set(tabs, row_sets);

			// Output this as debugging information
			if (DEBUG_QUERY) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
					            out_table + " = " + this + ".MergeWithReference(" +
					            ReferenceTable + ", " + max_column + " )");
				}
			}

			table = out_table;
			return table;
		}

		// ------ Package protected methods -----

		/// <summary>
		/// Returns a list of rows that represent one row from each distinct group
		/// in this table.
		/// </summary>
		/// <remarks>
		/// This should be used to construct a virtual table of rows from 
		/// each distinct group.
		/// </remarks>
		/// <returns></returns>
		private IntegerVector GetTopFromEachGroup() {
			IntegerVector extract_rows = new IntegerVector();
			int size = group_links.Count;
			bool take = true;
			for (int i = 0; i < size; ++i) {
				int r = group_links[i];
				if (take) {
					extract_rows.AddInt(r & 0x03FFFFFFF);
				}
				take = (r & 0x040000000) != 0;
			}

			return extract_rows;
		}


		/// <summary>
		/// Returns a list of rows that represent the maximum row of the given column
		/// from each distinct group in this table.
		/// </summary>
		/// <param name="col_num"></param>
		/// <remarks>
		/// This should be used to construct a virtual table of rows from 
		/// each distinct group.
		/// </remarks>
		/// <returns></returns>
		private IntegerVector GetMaxFromEachGroup(int col_num) {
			Table ref_tab = ReferenceTable;

			IntegerVector extract_rows = new IntegerVector();
			int size = group_links.Count;

			int to_take_in_group = -1;
			TObject max = null;

			bool take = true;
			for (int i = 0; i < size; ++i) {
				int r = group_links[i];

				int act_r_index = r & 0x03FFFFFFF;
				TObject cell = ref_tab.GetCellContents(col_num, act_r_index);
				if (max == null || cell.CompareTo(max) > 0) {
					max = cell;
					to_take_in_group = act_r_index;
				}
				if ((r & 0x040000000) != 0) {
					extract_rows.AddInt(to_take_in_group);
					max = null;
				}
			}

			return extract_rows;
		}

		// ------ Methods that are implemented for Table interface ------

		public override TObject GetCellContents(int column, int row) {
			// [ FUNCTION TABLE CACHING NOW USES THE GLOBAL CELL CACHING MECHANISM ]
			// Check if in the cache,
			DataCellCache cache = Database.DataCellCache;
			// Is the column worth caching, and is caching enabled?
			if (exp_info[column] == 0 && cache != null) {
				TObject cell = cache.Get(unique_id, row, column);
				if (cell != null) {
					// In the cache so return the cell.
					return cell;
				} else {
					// Not in the cache so calculate the value and WriteByte it in the cache.
					cell = CalcValue(column, row, cache);
					return cell;
				}
			} else {
				// Caching is not enabled
				return CalcValue(column, row, null);
			}
		}

		public override IRowEnumerator GetRowEnumerator() {
			return new SimpleRowEnumerator(row_count);
		}

		internal override void AddDataTableListener(IDataTableListener listener) {
			// Add a data table listener to the reference table.
			// NOTE: This will cause the reference table to have the same listener
			//   registered twice if the 'MergeWithReference' method is used.  While
			//   this isn't perfect behaviour, it means if 'MergeWithReference' isn't
			//   used, we still will be notified of changes in the reference table
			//   which will alter the values in this table.
			ReferenceTable.AddDataTableListener(listener);
		}

		internal override void RemoveDataTableListener(IDataTableListener listener) {
			// Removes a data table listener to the reference table.
			// ( see notes above... )
			ReferenceTable.RemoveDataTableListener(listener);
		}

		public override void LockRoot(int lock_key) {
			// We Lock the reference table.
			// NOTE: This cause the reference table to Lock twice when we use the
			//  'MergeWithReference' method.  While this isn't perfect behaviour, it
			//  means if 'MergeWithReference' isn't used, we still maintain a safe
			//  level of locking.
			ReferenceTable.LockRoot(lock_key);
		}

		public override void UnlockRoot(int lock_key) {
			// We unlock the reference table.
			// NOTE: This cause the reference table to unlock twice when we use the
			//  'MergeWithReference' method.  While this isn't perfect behaviour, it
			//  means if 'MergeWithReference' isn't used, we still maintain a safe
			//  level of locking.
			ReferenceTable.UnlockRoot(lock_key);
		}

		// ---------- Convenience statics ----------

		///<summary>
		/// Returns a FunctionTable that has a single Expression evaluated in it.
		///</summary>
		///<param name="context"></param>
		///<param name="expression"></param>
		/// <remarks>
		/// The column name is 'result'.
		/// </remarks>
		///<returns></returns>
		public static Table ResultTable(DatabaseQueryContext context, Expression expression) {
			Expression[] exp = new Expression[] { expression };
			string[] names = new String[] {"result"};
			Table function_table = new FunctionTable(exp, names, context);
			SubsetColumnTable result = new SubsetColumnTable(function_table);

			int[] map = new int[] {0};
			Variable[] vars = new Variable[] {new Variable("result")};
			result.SetColumnMap(map, vars);

			return result;
		}

		///<summary>
		/// Returns a FunctionTable that has a single TObject in it.
		///</summary>
		///<param name="context"></param>
		///<param name="ob"></param>
		/// <remarks>
		/// The column title is 'result'.
		/// </remarks>
		///<returns></returns>
		public static Table ResultTable(DatabaseQueryContext context, TObject ob) {
			Expression result_exp = new Expression();
			result_exp.AddElement(ob);
			return ResultTable(context, result_exp);
		}

		///<summary>
		/// Returns a FunctionTable that has a single Object in it.
		///</summary>
		///<param name="context"></param>
		///<param name="ob"></param>
		/// <remarks>
		/// The column title is 'result'.
		/// </remarks>
		///<returns></returns>
		public static Table ResultTable(DatabaseQueryContext context, Object ob) {
			return ResultTable(context, TObject.GetObject(ob));
		}

		///<summary>
		/// Returns a FunctionTable that has an int value made into a BigNumber.
		///</summary>
		///<param name="context"></param>
		///<param name="result_val"></param>
		/// <remarks>
		/// The column title is 'result'.
		/// </remarks>
		///<returns></returns>
		public static Table ResultTable(DatabaseQueryContext context, int result_val) {
			return ResultTable(context, (BigNumber) result_val);
		}


		// ---------- Inner classes ----------

		#region Nested type: TableGroupResolver

		/// <summary>
		/// Group resolver.  This is used to resolve group informations
		/// in the refering table.
		/// </summary>
		private sealed class TableGroupResolver : IGroupResolver {
			private readonly FunctionTable table;
			/**
			 * The IntegerVector that represents the group we are currently
			 * processing.
			 */
			private IntegerVector group;

			//    /**
			//     * The group row index we are current set at.
			//     */
			//    private int group_row_index;

			/**
			 * The current group number.
			 */
			private int group_number = -1;

			/**
			 * A IVariableResolver that can resolve variables within a set of a group.
			 */
			private TableGVResolver tgv_resolver;

			public TableGroupResolver(FunctionTable table) {
				this.table = table;
			}

			#region IGroupResolver Members

			public int GroupId {
				get { return group_number; }
			}

			public int Count {
				get {
					if (group_number == -2) {
						return table.whole_table_group_size;
						//        return whole_table_group.size();
						//        // ISSUE: Unsafe call if reference table is a DataTable.
						//        return getReferenceTable().getRowCount();
					} else if (group != null) {
						return group.Count;
					} else {
						return table.GetGroupSize(group_number);
					}
				}
			}

			public TObject Resolve(Variable variable, int set_index) {
				//      String col_name = variable.getName();

				int col_index = table.ReferenceTable.FastFindFieldName(variable);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + variable);
				}

				EnsureGroup();

				int row_index = set_index;
				if (group != null) {
					row_index = group[set_index];
				}
				TObject cell = table.ReferenceTable.GetCellContents(col_index, row_index);

				return cell;
			}

			public IVariableResolver GetVariableResolver(int set_index) {
				TableGVResolver resolver = CreateVariableResolver();
				resolver.SetIndex(set_index);
				return resolver;
			}

			#endregion

			/// <summary>
			/// Creates a resolver that resolves variables within a set of the group.
			/// </summary>
			/// <returns></returns>
			private TableGVResolver CreateVariableResolver() {
				if (tgv_resolver != null) {
					return tgv_resolver;
				}
				tgv_resolver = new TableGVResolver();
				return tgv_resolver;
			}


			/// <summary>
			/// Ensures that 'group' is set up.
			/// </summary>
			private void EnsureGroup() {
				if (group == null) {
					if (group_number == -2) {
						group = table.whole_table_group;
						//          // ISSUE: Unsafe calls if reference table is a DataTable.
						//          group = new IntegerVector(getReferenceTable().getRowCount());
						//          IRowEnumerator renum = getReferenceTable().GetRowEnumerator();
						//          while (renum.hasMoreRows()) {
						//            group.Add(renum.nextRowIndex());
						//          }
					} else {
						group = table.GetGroupRows(group_number);
					}
				}
			}

			/// <summary>
			/// Given a row index, this will setup the information in this resolver
			/// to solve for this group.
			/// </summary>
			/// <param name="row_index"></param>
			public void SetUpGroupForRow(int row_index) {
				if (table.whole_table_as_group) {
					if (group_number != -2) {
						group_number = -2;
						group = null;
					}
				} else {
					int g = table.GetRowGroup(row_index);
					if (g != group_number) {
						group_number = g;
						group = null;
					}
				}
			}

			// ---------- Inner classes ----------

			#region Nested type: TableGVResolver

			private class TableGVResolver : IVariableResolver {
				private int set_index;
				private TableGroupResolver tgr;

				// ---------- Implemented from IVariableResolver ----------

				#region IVariableResolver Members

				public int SetId {
					get { throw new ApplicationException("setID not implemented here..."); }
				}

				public TObject Resolve(Variable variable) {
					return tgr.Resolve(variable, set_index);
				}

				public TType ReturnTType(Variable variable) {
					int col_index = tgr.table.ReferenceTable.FastFindFieldName(variable);
					if (col_index == -1) {
						throw new ApplicationException("Can't find column: " + variable);
					}

					return tgr.table.ReferenceTable.DataTableDef[col_index].TType;
				}

				#endregion

				internal void SetIndex(int set_index) {
					this.set_index = set_index;
				}
			}

			#endregion
		}

		#endregion
	}
}