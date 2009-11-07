//  
//  RowData.cs
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
using System.Collections;
using System.Text;

namespace Deveel.Data {
	///<summary>
	/// Represents a row of data to be added into a table.
	///</summary>
	/// <remarks>
	/// There are two types of RowData object.  Those that are empty and contain
	/// blank data, and those that contain information to either be inserted
	/// into a table, or has be retrieved from a row.
	/// <para>
	/// Any <see cref="RowData"/> objects that need to be set to <c>null</c> should 
	/// be done so explicitly.
	/// </para>
	/// <para>
	/// We must call a <see cref="SetColumnData"/> method for <i>every</i> column in 
	/// the row to form.
	/// </para>
	/// <para>
	/// This method (or derived classes) must only use safe methods in <see cref="DataTable"/>.
	/// </para>
	/// </remarks>
	public class RowData {

		/// <summary>
		/// The TransactionSystem this RowData is a context of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The <see cref="ITableDataSource"/> object that this <see cref="RowData"/> 
		/// is in, or is destined to be in.
		/// </summary>
		private readonly ITableDataSource table;

		/// <summary>
		/// The definition of the table.
		/// </summary>
		private readonly DataTableDef table_def;

		/// <summary>
		/// A list of TObject objects in the table.
		/// </summary>
		private readonly TObject[] data_cell_list;

		/// <summary>
		/// The number of columns in the row.
		/// </summary>
		private readonly int col_count;

		/// <summary>
		/// Creates a <see cref="RowData"/> object without an underlying table.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="col_count"></param>
		/// <remarks>
		/// This is used for copying from one table to a different one.
		/// </remarks>
		public RowData(TransactionSystem system, int col_count) {
			this.system = system;
			this.col_count = col_count;
			data_cell_list = new TObject[col_count];
		}

		/// <summary>
		/// Creates a blank row on the given underlying table.
		/// </summary>
		/// <param name="table"></param>
		public RowData(ITableDataSource table) {
			system = table.System;
			this.table = table;
			table_def = table.DataTableDef;
			col_count = table_def.ColumnCount;
			data_cell_list = new TObject[col_count];
		}

		/// <summary>
		/// Populates the RowData object with information from a specific row 
		/// from the underlying DataTable.
		/// </summary>
		/// <param name="row"></param>
		internal void SetFromRow(int row) {
			for (int col = 0; col < col_count; ++col) {
				SetColumnData(col, table.GetCellContents(col, row));
			}
		}

		/// <summary>
		/// Returns the table object this row data is assigned to.
		/// </summary>
		/// <param name="tab"></param>
		/// <remarks>
		/// This is used to ensure we don't try to use a row data in a different table 
		/// to what it was created from.
		/// </remarks>
		/// <returns></returns>
		internal bool IsSameTable(DataTable tab) {
			return table == tab;
		}

		/// <summary>
		/// Sets up a column by casting the value from the given <see cref="TObject"/> 
		/// to a type that is compatible with the column.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="cell"></param>
		/// <remarks>
		/// This is useful when we are copying information from one table to another.
		/// </remarks>
		public void SetColumnData(int column, TObject cell) {
			DataTableColumnDef col = table_def[column];
			if (table != null && col.SqlType != cell.TType.SQLType) {
				// Cast the TObject
				cell = cell.CastTo(col.TType);
			}
			SetColumnDataFromTObject(column, cell);
		}

		///<summary>
		/// Sets up a column from an Object.
		///</summary>
		///<param name="column"></param>
		///<param name="ob"></param>
		public void SetColumnDataFromObject(int column, Object ob) {
			DataTableColumnDef col_def = table_def[column];

			if (ob is String)
				ob = StringObject.FromString((String)ob);

			// Create a TObject from the given object to the given type
			TObject cell = TObject.CreateAndCastFromObject(col_def.TType, ob);
			SetColumnDataFromTObject(column, cell);
		}

		///<summary>
		/// Sets up a column from a TObject.
		///</summary>
		///<param name="column"></param>
		///<param name="ob"></param>
		public void SetColumnDataFromTObject(int column, TObject ob) {
			data_cell_list[column] = ob;
		}

		///<summary>
		/// This is a special case situation for setting the column cell to 'null'.
		///</summary>
		///<param name="column"></param>
		public void SetToNull(int column) {
			DataTableColumnDef col_def = table_def[column];
			SetColumnDataFromTObject(column, new TObject(col_def.TType, null));
		}

		///<summary>
		/// Sets the given column number to the default value for this column.
		///</summary>
		///<param name="column"></param>
		///<param name="context"></param>
		public void SetToDefault(int column, IQueryContext context) {
			if (table != null) {
				DataTableColumnDef column_def = table_def[column];
				Expression exp = column_def.GetDefaultExpression(system);
				if (exp != null) {
					TObject def_val = Evaluate(exp, context);
					SetColumnData(column, def_val);
					return;
				}
			}
			SetToNull(column);
		}

		/// <summary>
		/// Returns the <see cref="TObject"/> that represents the information 
		/// in the given column of the row.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public TObject GetCellData(int column) {
			TObject cell = data_cell_list[column];
			if (cell == null) {
				DataTableColumnDef col_def = table_def[column];
				cell = new TObject(col_def.TType, null);
			}
			return cell;
		}

		///<summary>
		/// Returns the name of the given column number.
		///</summary>
		///<param name="column"></param>
		///<returns></returns>
		public String GetColumnName(int column) {
			return table_def[column].Name;
		}

		///<summary>
		/// Finds the field in this RowData with the given name.
		///</summary>
		///<param name="column_name"></param>
		///<returns></returns>
		public int FindFieldName(String column_name) {
			return table_def.FindColumnName(column_name);
		}

		/// <summary>
		/// Returns the number of columns (cells) in this row.
		/// </summary>
		public int ColumnCount {
			get { return col_count; }
		}

		/// <summary>
		/// Evaluates the expression and returns the object it evaluates to using
		/// the local <see cref="IVariableResolver"/> to resolve variables in 
		/// the expression.
		/// </summary>
		/// <param name="expression"></param>
		/// <param name="context"></param>
		/// <returns></returns>
		private TObject Evaluate(Expression expression, IQueryContext context) {
			bool ignore_case = system.IgnoreIdentifierCase;
			// Resolve any variables to the table_def for this expression.
			table_def.ResolveColumns(ignore_case, expression);
			// Get the variable resolver and evaluate over this data.
			IVariableResolver vresolver = VariableResolver;
			return expression.Evaluate(null, vresolver, context);
		}

		/// <summary>
		/// Evaluates a single assignment on this RowData object.
		/// </summary>
		/// <param name="assignment"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// A <see cref="IVariableResolver"/> is made which resolves to 
		/// variables only within this row context.
		/// </remarks>
		internal void Evaluate(Assignment assignment, IQueryContext context) {
			// Get the variable resolver and evaluate over this data.
			IVariableResolver vresolver = VariableResolver;
			TObject ob = assignment.Expression.Evaluate(null, vresolver, context);

			// Check the variable name is within this row.
			Variable variable = assignment.Variable;
			int column = FindFieldName(variable.Name);

			// Set the column to the resolved value.
			SetColumnData(column, ob);
		}

		/// <summary>
		/// Sets any columns in the row haven't been set yet (they will be
		/// <b>null</b>) to the default value.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// This should be called after the row data has initially been set 
		/// with values from some source.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void SetToDefault(IQueryContext context) {
			for (int i = 0; i < col_count; ++i) {
				if (data_cell_list[i] == null) {
					SetToDefault(i, context);
				}
			}
		}

		/// <summary>
		/// Sets up an entire row given the array of assignments.
		/// </summary>
		/// <param name="assignments"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// If any columns are left <b>null</b> then they are filled with 
		/// the default value.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void SetupEntire(Assignment[] assignments, IQueryContext context) {
			for (int i = 0; i < assignments.Length; ++i) {
				Evaluate(assignments[i], context);
			}
			// Any that are left as 'null', set to default value.
			SetToDefault(context);
		}

		/// <summary>
		/// Sets up an entire row given the list of insert elements and a list of
		/// indices to the columns to set.
		/// </summary>
		/// <param name="col_indices">Indices of the columns where to set the values.</param>
		/// <param name="insert_elements">List of all the elements to insert at the specified indices.</param>
		/// <param name="context"></param>
		/// <remarks>
		/// An insert element is either an expression that is resolved to a constant, 
		/// or the string "DEFAULT" which indicates the value should be set 
		/// to the default value of the column.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void SetupEntire(int[] col_indices, IList insert_elements, IQueryContext context) {
			int elem_size = insert_elements.Count;
			if (col_indices.Length != elem_size) {
				throw new DatabaseException(
							   "Column indices and expression array sizes don't match");
			}
			// Get the variable resolver and evaluate over this data.
			IVariableResolver vresolver = VariableResolver;
			for (int i = 0; i < col_indices.Length; ++i) {
				Object element = insert_elements[i];
				if (element is Expression) {
					// Evaluate to the object to insert
					TObject ob = ((Expression)element).Evaluate(null, vresolver, context);
					int table_column = col_indices[i];
					// Cast the object to the type of the column
					ob = ob.CastTo(table_def[table_column].TType);
					// Set the column to the resolved value.
					SetColumnDataFromTObject(table_column, ob);
				} else {
					// The element must be 'DEFAULT'.  If it's not throw an error.  If it
					// is, the default value will be set later.
					if (!element.Equals("DEFAULT")) {
						throw new DatabaseException(
												"Invalid value in 'insert_elements' list.");
					}
				}
			}
			// Any that are left as 'null', set to default value.
			SetToDefault(context);
		}

		///<summary>
		/// Sets up an entire row given the array of Expressions and a list of indices
		/// to the columns to set.
		///</summary>
		///<param name="col_indices"></param>
		///<param name="exps"></param>
		///<param name="context"></param>
		/// <remarks>
		/// Any columns that are not set by this method are set to the default 
		/// value as defined for the column.
		/// </remarks>
		public void SetupEntire(int[] col_indices, Expression[] exps, IQueryContext context) {
			SetupEntire(col_indices, (IList)exps, context);
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[RowData: ");
			for (int i = 0; i < col_count; ++i) {
				buf.Append(data_cell_list[i].Object);
				buf.Append(", ");
			}
			return buf.ToString();
		}

		/// <summary>
		/// Returns a IVariableResolver to use within this RowData context.
		/// </summary>
		private IVariableResolver VariableResolver {
			get {
				if (variable_resolver == null) {
					variable_resolver = new RDVariableResolver(this);
				} else {
					variable_resolver.NextAssignment();
				}
				return variable_resolver;
			}
		}

		private RDVariableResolver variable_resolver = null;

		// ---------- Inner classes ----------

		/// <summary>
		/// Variable resolver for this context.
		/// </summary>
		private class RDVariableResolver : IVariableResolver {
			private readonly RowData row_data;
			private int assignment_count = 0;

			public RDVariableResolver(RowData rowData) {
				row_data = rowData;
			}

			internal void NextAssignment() {
				++assignment_count;
			}

			public int SetId {
				get { return assignment_count; }
			}

			public TObject Resolve(Variable variable) {
				String col_name = variable.Name;

				int col_index = row_data.table_def.FindColumnName(col_name);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + col_name);
				}

				TObject cell = row_data.data_cell_list[col_index];

				if (cell == null) {
					throw new ApplicationException("Column " + col_name + " hasn't been set yet.");
				}

				return cell;
			}

			public TType ReturnTType(Variable variable) {
				String col_name = variable.Name;

				int col_index = row_data.table_def.FindColumnName(col_name);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + col_name);
				}

				return row_data.table_def[col_index].TType;
			}

		}
	}
}