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
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
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
	public sealed class DataRow {

		/// <summary>
		/// The TransactionSystem this DataRow is a context of.
		/// </summary>
		private readonly SystemContext context;

		/// <summary>
		/// The <see cref="ITableDataSource"/> object that this <see cref="DataRow"/> 
		/// is in, or is destined to be in.
		/// </summary>
		private readonly ITableDataSource table;

		/// <summary>
		/// The definition of the table.
		/// </summary>
		private readonly DataTableInfo tableInfo;

		/// <summary>
		/// A list of TObject objects in the table.
		/// </summary>
		private readonly TObject[] dataCellList;

		/// <summary>
		/// The number of columns in the row.
		/// </summary>
		private readonly int colCount;

		/// <summary>
		/// Creates a <see cref="DataRow"/> object without an underlying table.
		/// </summary>
		/// <param name="context></param>
		/// <param name="colCount"></param>
		/// <remarks>
		/// This is used for copying from one table to a different one.
		/// </remarks>
		internal DataRow(SystemContext context, int colCount) {
			this.context = context;
			this.colCount = colCount;
			dataCellList = new TObject[colCount];
		}

		/// <summary>
		/// Creates a blank row on the given underlying table.
		/// </summary>
		/// <param name="table"></param>
		internal DataRow(ITableDataSource table) {
			context = table.Context;
			this.table = table;
			tableInfo = table.TableInfo;
			colCount = tableInfo.ColumnCount;
			dataCellList = new TObject[colCount];
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
			for (int col = 0; col < colCount; ++col) {
				SetValue(col, table.GetCell(col, row));
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
				DataColumnInfo colInfo = tableInfo[column];

				if (value is String)
					value = StringObject.FromString((string)value);

				// Create a TObject from the given object to the given type
				value = TObject.CreateAndCastFromObject(colInfo.TType, value);
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
			DataColumnInfo col = tableInfo[column];
			if (table != null && col.SqlType != value.TType.SqlType) {
				// Cast the TObject
				value = value.CastTo(col.TType);
			}

			dataCellList[column] = value;
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
			DataColumnInfo colInfo = tableInfo[column];
			SetValue(column, new TObject(colInfo.TType, null));
		}

		///<summary>
		/// Sets the given column number to the default value for this column.
		///</summary>
		///<param name="column"></param>
		///<param name="context"></param>
		public void SetToDefault(int column, IQueryContext context) {
			if (table != null) {
				DataColumnInfo columnInfo = tableInfo[column];
				Expression exp = columnInfo.GetDefaultExpression(this.context);
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
			TObject cell = dataCellList[column];
			if (cell == null) {
				DataColumnInfo colInfo = tableInfo[column];
				cell = new TObject(colInfo.TType, null);
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
			return tableInfo[column].Name;
		}

		///<summary>
		/// Finds the field in this DataRow with the given name.
		///</summary>
		///<param name="columnName"></param>
		///<returns></returns>
		public int FindFieldName(string columnName) {
			return tableInfo.FindColumnName(columnName);
		}

		/// <summary>
		/// Returns the number of columns (cells) in this row.
		/// </summary>
		public int ColumnCount {
			get { return colCount; }
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
			bool ignoreCase = this.context.IgnoreIdentifierCase;
			// Resolve any variables to the table_def for this expression.
			tableInfo.ResolveColumns(ignoreCase, expression);
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
			for (int i = 0; i < colCount; ++i) {
				if (dataCellList[i] == null) {
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
		/// <param name="colIndices">Indices of the columns where to set the values.</param>
		/// <param name="insertElements">List of all the elements to insert at the specified indices.</param>
		/// <param name="context"></param>
		/// <remarks>
		/// An insert element is either an expression that is resolved to a constant, 
		/// or the string "DEFAULT" which indicates the value should be set 
		/// to the default value of the column.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void SetupEntire(int[] colIndices, IList insertElements, IQueryContext context) {
			int elemSize = insertElements.Count;
			if (colIndices.Length != elemSize)
				throw new DatabaseException("Column indices and expression array sizes don't match");

			// Get the variable resolver and evaluate over this data.
			IVariableResolver vresolver = VariableResolver;
			for (int i = 0; i < colIndices.Length; ++i) {
				Object element = insertElements[i];
				if (element is Expression) {
					// Evaluate to the object to insert
					TObject ob = ((Expression)element).Evaluate(null, vresolver, context);
					int tableColumn = colIndices[i];
					// Cast the object to the type of the column
					ob = ob.CastTo(tableInfo[tableColumn].TType);
					// Set the column to the resolved value.
					SetValue(tableColumn, ob);
				} else {
					// The element must be 'DEFAULT'.  If it's not throw an error.  If it
					// is, the default value will be set later.
					if (!element.Equals("DEFAULT"))
						throw new DatabaseException("Invalid value in 'insert_elements' list.");
				}
			}
			// Any that are left as 'null', set to default value.
			SetToDefault(context);
		}

		///<summary>
		/// Sets up an entire row given the array of Expressions and a list of indices
		/// to the columns to set.
		///</summary>
		///<param name="colIndices"></param>
		///<param name="exps"></param>
		///<param name="context"></param>
		/// <remarks>
		/// Any columns that are not set by this method are set to the default 
		/// value as defined for the column.
		/// </remarks>
		public void SetupEntire(int[] colIndices, Expression[] exps, IQueryContext context) {
			SetupEntire(colIndices, (IList)exps, context);
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[DataRow: ");
			for (int i = 0; i < colCount; ++i) {
				buf.Append(dataCellList[i].Object);
				buf.Append(", ");
			}
			return buf.ToString();
		}

		/// <summary>
		/// Returns a IVariableResolver to use within this DataRow context.
		/// </summary>
		private IVariableResolver VariableResolver {
			get {
				if (variableResolver == null) {
					variableResolver = new DRVariableResolver(this);
				} else {
					variableResolver.NextAssignment();
				}
				return variableResolver;
			}
		}

		private DRVariableResolver variableResolver = null;

		// ---------- Inner classes ----------

		/// <summary>
		/// Variable resolver for this context.
		/// </summary>
		private class DRVariableResolver : IVariableResolver {
			private readonly DataRow dataRow;
			private int assignmentCount = 0;

			public DRVariableResolver(DataRow dataRow) {
				this.dataRow = dataRow;
			}

			internal void NextAssignment() {
				++assignmentCount;
			}

			public int SetId {
				get { return assignmentCount; }
			}

			public TObject Resolve(VariableName variable) {
				string colName = variable.Name;

				int colIndex = dataRow.tableInfo.FindColumnName(colName);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column: " + colName);

				TObject cell = dataRow.dataCellList[colIndex];

				if (cell == null)
					throw new ApplicationException("Column " + colName + " hasn't been set yet.");

				return cell;
			}

			public TType ReturnTType(VariableName variable) {
				string colName = variable.Name;

				int colIndex = dataRow.tableInfo.FindColumnName(colName);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column: " + colName);

				return dataRow.tableInfo[colIndex].TType;
			}

		}
	}
}