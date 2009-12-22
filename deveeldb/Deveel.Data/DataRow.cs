//  
//  DataRow.cs
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
	/// There are two types of DataRow object.  Those that are empty and contain
	/// blank data, and those that contain information to either be inserted
	/// into a table, or has be retrieved from a row.
	/// <para>
	/// Any <see cref="DataRow"/> objects that need to be set to <c>null</c> should 
	/// be done so explicitly.
	/// </para>
	/// <para>
	/// We must call a <c>SetValue</c> method for <i>every</i> column in the row to form.
	/// </para>
	/// <para>
	/// This method (or derived classes) must only use safe methods in <see cref="DataTable"/>.
	/// </para>
	/// </remarks>
	public class DataRow {

		/// <summary>
		/// The TransactionSystem this DataRow is a context of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The <see cref="ITableDataSource"/> object that this <see cref="DataRow"/> 
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
		/// Creates a <see cref="DataRow"/> object without an underlying table.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="col_count"></param>
		/// <remarks>
		/// This is used for copying from one table to a different one.
		/// </remarks>
		internal DataRow(TransactionSystem system, int col_count) {
			this.system = system;
			this.col_count = col_count;
			data_cell_list = new TObject[col_count];
		}

		/// <summary>
		/// Creates a blank row on the given underlying table.
		/// </summary>
		/// <param name="table"></param>
		internal DataRow(ITableDataSource table) {
			system = table.System;
			this.table = table;
			table_def = table.DataTableDef;
			col_count = table_def.ColumnCount;
			data_cell_list = new TObject[col_count];
		}

		/// <summary>
		/// Gets or sets the value of the column at the given
		/// column index.
		/// </summary>
		/// <param name="column">The index of the column to get
		/// or set the value of a column.</param>
		/// <returns>
		/// Return a <see cref="TObject"/> that encapsulates the value
		/// of the column at the given index of the row.
		/// </returns>
		public TObject this[int column] {
			get { return GetValue(column); }
			set { SetValue(column, value); }
		}

		/// <summary>
		/// Populates the DataRow object with information from a specific row 
		/// from the underlying DataTable.
		/// </summary>
		/// <param name="row"></param>
		internal void SetFromRow(int row) {
			for (int col = 0; col < col_count; ++col) {
				SetValue(col, table.GetCellContents(col, row));
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
		/// Sets the value of a column for this row.
		/// </summary>
		/// <param name="column">The zero-based index of the column for which
		/// to set the value.</param>
		/// <param name="value">The object value to set: if not an instance
		/// of <see cref="TObject"/>, it will be casted.</param>
		/// <remarks>
		/// This overload of the method of <c>SetValue</c> method accepts a
		/// generic value parameter that can be every kind of (allowed) data
		/// type from the framework: if not an instance of <see cref="TObject"/>
		/// this method will cast it accordingly to the definition of the
		/// data type of the column.
		/// </remarks>
		public void SetValue(int column, object value) {
			if (!typeof (TObject).IsInstanceOfType(value)) {
				DataTableColumnDef col_def = table_def[column];

				if (value is String)
					value = StringObject.FromString((string)value);

				// Create a TObject from the given object to the given type
				value = TObject.CreateAndCastFromObject(col_def.TType, value);
			}

			SetValue(column, (TObject)value);
		}

		/// <summary>
		/// Sets the value of a column for this row.
		/// </summary>
		/// <param name="column">The zero-based index of the column for which
		/// to set the value.</param>
		/// <param name="value">The value to set for the column.</param>
		public void SetValue(int column, TObject value) {
			DataTableColumnDef col = table_def[column];
			if (table != null && col.SqlType != value.TType.SQLType) {
				// Cast the TObject
				value = value.CastTo(col.TType);
			}

			data_cell_list[column] = value;
		}

		public void SetValue(string columnName, TObject value) {
			int columnIndex = FindFieldName(columnName);
			if (columnIndex == -1)
				throw new ArgumentException("Cannot find the column '" + columnName + "' in the table.");

			SetValue(columnIndex, value);
		}

		///<summary>
		/// This is a special case situation for setting the column cell to 'null'.
		///</summary>
		///<param name="column"></param>
		public void SetToNull(int column) {
			DataTableColumnDef col_def = table_def[column];
			SetValue(column, new TObject(col_def.TType, null));
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
					SetValue(column, def_val);
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
		public TObject GetValue(int column) {
			TObject cell = data_cell_list[column];
			if (cell == null) {
				DataTableColumnDef col_def = table_def[column];
				cell = new TObject(col_def.TType, null);
			}
			return cell;
		}

		public TObject GetValue(string columnName) {
			int columnIndex = FindFieldName(columnName);
			if (columnIndex == -1)
				throw new ArgumentException("Cannot find the column '" + columnName + "' in the table.");

			return GetValue(columnIndex);
		}

		///<summary>
		/// Returns the name of the given column number.
		///</summary>
		///<param name="column"></param>
		///<returns></returns>
		public string GetColumnName(int column) {
			return table_def[column].Name;
		}

		///<summary>
		/// Finds the field in this DataRow with the given name.
		///</summary>
		///<param name="columnName"></param>
		///<returns></returns>
		public int FindFieldName(string columnName) {
			return table_def.FindColumnName(columnName);
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
		/// Evaluates a single assignment on this DataRow object.
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
			VariableName variable = assignment.VariableName;
			int column = FindFieldName(variable.Name);

			// Set the column to the resolved value.
			SetValue(column, ob);
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
					SetValue(table_column, ob);
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
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[DataRow: ");
			for (int i = 0; i < col_count; ++i) {
				buf.Append(data_cell_list[i].Object);
				buf.Append(", ");
			}
			return buf.ToString();
		}

		/// <summary>
		/// Returns a IVariableResolver to use within this DataRow context.
		/// </summary>
		private IVariableResolver VariableResolver {
			get {
				if (variable_resolver == null) {
					variable_resolver = new DRVariableResolver(this);
				} else {
					variable_resolver.NextAssignment();
				}
				return variable_resolver;
			}
		}

		private DRVariableResolver variable_resolver = null;

		// ---------- Inner classes ----------

		/// <summary>
		/// Variable resolver for this context.
		/// </summary>
		private class DRVariableResolver : IVariableResolver {
			private readonly DataRow dataRow;
			private int assignment_count = 0;

			public DRVariableResolver(DataRow dataRow) {
				this.dataRow = dataRow;
			}

			internal void NextAssignment() {
				++assignment_count;
			}

			public int SetId {
				get { return assignment_count; }
			}

			public TObject Resolve(VariableName variable) {
				String col_name = variable.Name;

				int col_index = dataRow.table_def.FindColumnName(col_name);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + col_name);
				}

				TObject cell = dataRow.data_cell_list[col_index];

				if (cell == null) {
					throw new ApplicationException("Column " + col_name + " hasn't been set yet.");
				}

				return cell;
			}

			public TType ReturnTType(VariableName variable) {
				String col_name = variable.Name;

				int col_index = dataRow.table_def.FindColumnName(col_name);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + col_name);
				}

				return dataRow.table_def[col_index].TType;
			}

		}
	}
}