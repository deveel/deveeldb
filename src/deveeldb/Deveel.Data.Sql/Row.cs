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

using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A single row in a table of a database.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This object is a convenience to access the data provided
	/// by a <see cref="ITable"/> implementation, preserving the
	/// behavior of a tabular model.
	/// </para>
	/// <para>
	/// Any row in a database can be identified by a unique <see cref="RowId"/>
	/// coordinate, formed by the <see cref="TableInfo.Id">id of the table</see>
	/// that contains it and its unique number within the table.
	/// </para>
	/// </remarks>
	/// <seealso cref="ITable.GetValue"/>
	/// <seealso cref="TableInfo.Id"/>
	/// <seealso cref="RowId"/>
	public sealed class Row {
		private RowVariableResolver variableResolver;
		private Dictionary<int, DataObject> values;

		/// <summary>
		/// Constructs a new row object for the table given,
		/// identified by the given <see cref="RowId"/>.
		/// </summary>
		/// <param name="table">The table that contains the row.</param>
		/// <param name="rowId">The unique identifier of the row within the database.</param>
		/// <exception cref="ArgumentNullException">
		/// If the provided <paramref name="table"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the <paramref name="table"/> defines any id and this does not
		/// match the given <paramref name="rowId"/>.
		/// </exception>
		public Row(ITable table, RowId rowId) {
			if (table == null)
				throw new ArgumentNullException("table");

			if (!RowId.IsNull &&
				table.TableInfo.Id >= 0 &&
			    table.TableInfo.Id != rowId.TableId)
				throw new ArgumentException(String.Format("The table ID {0} does not match the identifier specified by the row ID ({1})",
					table.TableInfo.Id,
					rowId.TableId));

			Table = table;
			RowId = rowId;
		}

		public Row(ITable table, int rowNumber)
			: this(table, new RowId(table.TableInfo.Id, rowNumber)) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="table"></param>
		public Row(ITable table)
			: this(table, RowId.Null) {
		}

		/// <summary>
		/// Gets the instance of the table that contains the row.
		/// </summary>
		public ITable Table { get; private set; }

		/// <summary>
		/// Gets the row unique identifier within the database.
		/// </summary>
		public RowId RowId { get; private set; }

		/// <summary>
		/// Gets or sets the value of a cell of the row at the given offset.
		/// </summary>
		/// <param name="columnOffset">The zero-based column offset identifying the
		/// value to get from the table.</param>
		/// <returns>
		/// Returns a <see cref="DataObject"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <seealso cref="GetValue(int)"/>
		/// <seealso cref="SetValue(int, DataObject)"/>
		public DataObject this[int columnOffset] {
			get { return GetValue(columnOffset); }
			set { SetValue(columnOffset, value); }
		}

		/// <summary>
		/// Gets or sets the value of a cell of the row for the given column.
		/// </summary>
		/// <param name="columnName">The name of the column in the table.</param>
		/// <returns>
		/// Returns a <see cref="DataObject"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <seealso cref="GetValue(string)"/>
		/// <seealso cref="SetValue(string, DataObject)"/>
		public DataObject this[string columnName] {
			get { return GetValue(columnName); }
			set { SetValue(columnName, value); }
		}

		private IVariableResolver VariableResolver {
			get {
				if (variableResolver == null) {
					variableResolver = new RowVariableResolver(this);
				} else {
					variableResolver.NextAssignment();
				}
				return variableResolver;
			}
		}

		/// <summary>
		/// Gets or the value of a cell of the row at the given offset.
		/// </summary>
		/// <param name="columnOffset">The zero-based column offset identifying the
		/// value to get from the table.</param>
		/// <remarks>
		/// This methods keeps a cached versions of the values after the first time
		/// it is called: that means the 
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataObject"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="columnOffset"/> is smaller than zero
		/// or greater than the number of columns in the table.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the <see cref="RowId">id</see> of this row does not point to any row
		/// in the underlying table.
		/// </exception>
		/// <seealso cref="TableInfo.ColumnCount"/>
		public DataObject GetValue(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (RowId.IsNull)
				throw new InvalidOperationException("Row was not established to any table.");

			if (values == null) {
				var colCount = Table.TableInfo.ColumnCount;
				values = new Dictionary<int, DataObject>(colCount);

				for (int i = 0; i < colCount; i++) {
					values[i] = Table.GetValue(RowId.RowNumber, i);
				}
			}

			DataObject value;
			if (!values.TryGetValue(columnOffset, out value)) {
				var columnType = Table.TableInfo[columnOffset].ColumnType;
				return DataObject.Null(columnType);
			}

			return value;
		}

		/// <summary>
		/// Sets the value of a cell of the row at the given offset.
		/// </summary>
		/// <param name="columnOffset">The zero-based column offset identifying the
		/// value to get from the table.</param>
		/// <param name="value">The value to set to the cell.</param>
		/// <returns>
		/// Returns a <see cref="DataObject"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="columnOffset"/> is smaller than zero
		/// or greater than the number of columns in the table.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the <see cref="RowId">id</see> of this row does not point to any row
		/// in the underlying table.
		/// </exception>
		/// <seealso cref="TableInfo.ColumnCount"/>
		public void SetValue(int columnOffset, DataObject value) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (RowId.IsNull)
				throw new InvalidOperationException("Row was not established to any table.");

			if (values == null) {
				var colCount = Table.TableInfo.ColumnCount;
				values = new Dictionary<int, DataObject>(colCount);
			}

			values[columnOffset] = value;
		}

		public DataObject GetValue(string columnName) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			var offset = Table.TableInfo.IndexOfColumn(columnName);
			if (offset < 0)
				throw new ArgumentException(String.Format("Could not find column '{0}' in the table '{1}'.", columnName, Table.FullName));

			return GetValue(offset);
		}

		public void SetValue(string columnName, DataObject value) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			var offset = Table.TableInfo.IndexOfColumn(columnName);
			if (offset < 0)
				throw new ArgumentException(String.Format("Could not find column '{0}' in the table '{1}'.", columnName, Table.FullName));

			SetValue(offset, value);
		}

		/// <summary>
		/// Sets the value of a cell of the row at the given offset to <c>NULL</c>.
		/// </summary>
		/// <param name="columnOffset">The zero based offset of the column.</param>
		/// <seealso cref="SetValue(int, DataObject)"/>
		public void SetNull(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var columnType = Table.TableInfo[columnOffset].ColumnType;
			SetValue(columnOffset, new DataObject(columnType, null));
		}

		/// <summary>
		/// Sets the value of a cell of the row at the given offset to the
		/// <c>DEFAULT</c> set at the column definition.
		/// </summary>
		/// <param name="columnOffset">The zero based offset of the column.</param>
		/// <param name="context">The context that is used to evaluate the
		/// <c>DEFAULT</c> <see cref="SqlExpression"/> of the column.</param>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="columnOffset"/> is smaller than zero
		/// or greater than the number of columns in the table.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the column has no <c>DEFAULT</c> value defined.
		/// </exception>
		public void SetDefault(int columnOffset, IQueryContext context) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var column = Table.TableInfo[columnOffset];
			if (!column.HasDefaultExpression)
				throw new InvalidOperationException(String.Format("Column '{0}' in table '{1}' has no DEFAULT set.", column.ColumnName, Table.FullName));

			var value = Evaluate(column.DefaultExpression, context);
			SetValue(columnOffset, value);
		}

		private DataObject Evaluate(SqlExpression expression, IQueryContext queryContext) {
			// TODO: bool ignoreCase = queryContext.SystemContext.Config.IgnoreIdentifierCase();
			bool ignoreCase = true;
			// Resolve any variables to the table_def for this expression.
			expression = Table.TableInfo.ResolveColumns(ignoreCase, expression);
			// Get the variable resolver and evaluate over this data.
			IVariableResolver vresolver = VariableResolver;
			var reduced = expression.Evaluate(queryContext, vresolver, null);
			if (reduced.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException("The DEFAULT expression of the column cannot be reduced to a constant");

			return ((SqlConstantExpression) reduced).Value;
		}

		/// <summary>
		/// Sets the row number part of the identificator
		/// </summary>
		/// <param name="number">The unique row number within the underlying table</param>
		/// <remarks>
		/// <para>
		/// When a row is made permanent in a table (by a call to <see cref="IMutableTable.AddRow"/>),
		/// this method is called to update the address of the 
		/// </para>
		/// <para>
		/// If a row has no number locator to the underlying table all access
		/// methods will throw exceptions.
		/// </para>
		/// </remarks>
		public void SetNumber(int number) {
			RowId = new RowId(Table.TableInfo.Id, number);
		}

		#region RowVariableResolver

		private class RowVariableResolver : IVariableResolver {
			private readonly Row row;
			private int assignmentCount;

			public RowVariableResolver(Row row) {
				this.row = row;
			}

			internal void NextAssignment() {
				++assignmentCount;
			}

			public int SetId {
				get { return assignmentCount; }
			}

			public DataObject Resolve(ObjectName columnName) {
				string colName = columnName.Name;

				int colIndex = row.Table.TableInfo.IndexOfColumn(colName);
				if (colIndex == -1)
					throw new ApplicationException(String.Format("Column '{0}' could not be found in table '{1}'", colName, row.Table.FullName));

				DataObject value;
				if (!row.values.TryGetValue(colIndex, out value))
					throw new ApplicationException("Column " + colName + " hasn't been set yet.");

				return value;
			}

			public DataType ReturnType(ObjectName columnName) {
				string colName = columnName.Name;

				int colIndex = row.Table.TableInfo.IndexOfColumn(colName);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column: " + colName);

				return row.Table.TableInfo[colIndex].ColumnType;
			}
		}

		#endregion
	}
}