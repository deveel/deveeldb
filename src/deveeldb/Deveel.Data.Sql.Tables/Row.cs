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

using Deveel.Data.Design;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Tables {
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
	public sealed class Row : IDbObject {
		private RowVariableResolver variableResolver;
		private Dictionary<int, Field> values;

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

			if (!rowId.IsNull &&
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
		/// Constructs a new row on the given table that is not
		/// established into that table.
		/// </summary>
		/// <param name="table">The parent table of the row.</param>
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
		/// <remarks>
		/// When a row is not established in the parent table, this
		/// value is <see cref="Tables.RowId.Null"/>.
		/// </remarks>
		public RowId RowId { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return new RowInfo(this); }
		}

		/// <summary>
		/// Gets or sets the value of a cell of the row at the given offset.
		/// </summary>
		/// <param name="columnOffset">The zero-based column offset identifying the
		/// value to get from the table.</param>
		/// <returns>
		/// Returns a <see cref="Field"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <seealso cref="GetValue(int)"/>
		/// <seealso cref="SetValue(int, Field)"/>
		public Field this[int columnOffset] {
			get { return GetValue(columnOffset); }
			set { SetValue(columnOffset, value); }
		}

		/// <summary>
		/// Gets or sets the value of a cell of the row for the given column.
		/// </summary>
		/// <param name="columnName">The name of the column in the table.</param>
		/// <returns>
		/// Returns a <see cref="Field"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <seealso cref="GetValue(string)"/>
		/// <seealso cref="SetValue(string, Field)"/>
		public Field this[string columnName] {
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
		/// Gets the number of columns in the parent table of this row.
		/// </summary>
		public int ColumnCount {
			get { return Table.TableInfo.ColumnCount; }
		}

		/// <summary>
		/// Gets a boolean value indicating if the column exists in the
		/// parent table.
		/// </summary>
		public bool Exists {
			get { return Table.Any(x => x.RowId.Equals(RowId)); }
		}

		private bool? canBeCached;

		public bool CanBeCached {
			get {
				if (canBeCached == null) {
					canBeCached = values.All(value => value.Value.IsCacheable);
				}

				return canBeCached.Value;
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
		/// Returns a <see cref="Field"/> that encapsulates the type and value
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
		public Field GetValue(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (values == null) {
				if (RowId.IsNull)
					throw new InvalidOperationException("Row was not established to any table.");

				var colCount = Table.TableInfo.ColumnCount;
				values = new Dictionary<int, Field>(colCount);

				for (int i = 0; i < colCount; i++) {
					values[i] = Table.GetValue(RowId.RowNumber, i);
				}
			}

			Field value;
			if (!values.TryGetValue(columnOffset, out value)) {
				var columnType = Table.TableInfo[columnOffset].ColumnType;
				return Field.Null(columnType);
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
		/// Returns a <see cref="Field"/> that encapsulates the type and value
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
		public void SetValue(int columnOffset, Field value) {
			var colCount = Table.TableInfo.ColumnCount;

			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (values == null)
				values = new Dictionary<int, Field>(colCount);

			var column = Table.TableInfo[columnOffset];
			var columnType = column.ColumnType;

			if (value.IsNull && column.IsNotNull)
				throw new NotNullColumnViolationException(Table.TableInfo.TableName, column.ColumnName);

			if (!value.Type.Equals(columnType)) {
				if (!value.Type.CanCastTo(columnType))
					throw new ArgumentException(String.Format("The value of type {0} cannot be casted to column type {1}.", value.Type, columnType));

				value = value.CastTo(columnType);
			}

			values[columnOffset] = value;
		}

		/// <summary>
		/// Gets a value for a cell of the row that corresponds to the
		/// column of the table with the given name.
		/// </summary>
		/// <param name="columnName">The name of the column corresponding to
		/// the cell to get.</param>
		/// <returns>
		/// Returns a <see cref="Field"/> that encapsulates the type and value
		/// of the cell stored in the database.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="columnName"/> is empty or <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If no column with the given name was found in the parent table.
		/// </exception>
		/// <seealso cref="GetValue(int)"/>
		public Field GetValue(string columnName) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			var offset = Table.TableInfo.IndexOfColumn(columnName);
			if (offset < 0)
				throw new ArgumentException(String.Format("Could not find column '{0}' in the table '{1}'.", columnName, Table.TableInfo.TableName));

			return GetValue(offset);
		}

		public void SetValue(string columnName, Field value) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			var offset = Table.TableInfo.IndexOfColumn(columnName);
			if (offset < 0)
				throw new ArgumentException(String.Format("Could not find column '{0}' in the table '{1}'.", columnName, Table.TableInfo.TableName));

			SetValue(offset, value);
		}

		public void SetValue(int columnIndex, string value) {
			SetValue(columnIndex, Field.String(value));
		}

		public void SetValue(int columnIndex, int value) {
			SetValue(columnIndex, Field.Integer(value));
		}

		public void SetValue(int columnIndex, long value) {
			SetValue(columnIndex, Field.BigInt(value));
		}

		public void SetValue(int columnIndex, short value) {
			SetValue(columnIndex, Field.SmallInt(value));
		}

		public void SetValue(int columnIndex, float value) {
			SetValue(columnIndex, new SqlNumber(value));
		}

		public void SetValue(int columnIndex, double value) {
			SetValue(columnIndex, Field.Number(new SqlNumber(value)));
		}

		public void SetValue(int columnIndex, SqlNumber value) {
			SetValue(columnIndex, Field.Number(value));
		}

		public void SetValue(int columnIndex, bool value) {
			SetValue(columnIndex, Field.Boolean(value));
		}

		public void SetValue(int columnIndex, byte[] bytes) {
			SetValue(columnIndex, Field.Binary(bytes));
		}

		public void SetValue(int columnIndex, SqlBinary binary) {
			SetValue(columnIndex, Field.Binary(binary));
		}

		/// <summary>
		/// Sets the value of a cell of the row at the given offset to <c>NULL</c>.
		/// </summary>
		/// <param name="columnOffset">The zero based offset of the column.</param>
		/// <seealso cref="SetValue(int, Field)"/>
		public void SetNull(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var columnType = Table.TableInfo[columnOffset].ColumnType;
			SetValue(columnOffset, Field.Null(columnType));
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
		public void SetDefault(int columnOffset, IRequest context) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var column = Table.TableInfo[columnOffset];
			if (!column.HasDefaultExpression &&
				column.IsNotNull)
				throw new InvalidOperationException(String.Format("Column '{0}' in table '{1}' has no DEFAULT set.", column.ColumnName, Table.TableInfo.TableName));

			Field value;
			if (column.HasDefaultExpression) {
				value = Evaluate(column.DefaultExpression, context);
			} else {
				value = Field.Null(column.ColumnType);
			}

			SetValue(columnOffset, value);
		}

		/// <summary>
		/// Sets the <c>DEFAULT</c> value of all cells in the row as
		/// configured in the columns definition corresponding to the cells.
		/// </summary>
		/// <param name="context">The context that is used to evaluate the
		/// <c>DEFAULT</c> <see cref="SqlExpression"/> of the column.</param>
		/// <seealso cref="SetDefault(int, IRequest)"/>
		public void SetDefault(IRequest context) {
			for (int i = 0; i < Table.TableInfo.ColumnCount; ++i) {
				if (!values.ContainsKey(i)) {
					SetDefault(i, context);
				}
			}
		}

		private Field Evaluate(SqlExpression expression, IRequest queryContext) {
			var ignoreCase = queryContext.Query.IgnoreIdentifiersCase();
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

		public object ToObject(CompiledModel model, Type destType) {
			return model.ToObject(destType, this);
		}

		public T ToObject<T>(CompiledModel model) {
			return (T) model.ToObject(typeof(T), this);
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

			public Variable Resolve(ObjectName columnName) {
				string colName = columnName.Name;

				int colIndex = row.Table.TableInfo.IndexOfColumn(colName);
				if (colIndex == -1)
					throw new InvalidOperationException(String.Format("Column '{0}' could not be found in table '{1}'", colName, row.Table.TableInfo.TableName));

				Field value;
				if (!row.values.TryGetValue(colIndex, out value))
					throw new InvalidOperationException("Column " + colName + " hasn't been set yet.");

				var columnType = row.Table.TableInfo[colIndex].ColumnType;
				return new Variable(new VariableInfo(columnName.FullName, columnType, true), value);
			}

			public SqlType ReturnType(ObjectName columnName) {
				string colName = columnName.Name;

				int colIndex = row.Table.TableInfo.IndexOfColumn(colName);
				if (colIndex == -1)
					throw new InvalidOperationException("Can't find column: " + colName);

				return row.Table.TableInfo[colIndex].ColumnType;
			}
		}

		#endregion

		/// <summary>
		/// Sets the number component of the ID of this column.
		/// </summary>
		/// <param name="rowNumber">The zero-based number to set
		/// for identifying this row.</param>
		/// <seealso cref="Tables.RowId.RowNumber"/>
		/// <seealso cref="RowId"/>
		public void SetRowNumber(int rowNumber) {
			RowId = new RowId(Table.TableInfo.Id, rowNumber);
		}

		/// <summary>
		/// Gathers the original values from the table for the
		/// row number corresponding and populates this row
		/// with those values, making it ready to be accessed.
		/// </summary>
		/// <seealso cref="SetValue(int, Field)"/>
		public void SetFromTable() {
			for (int i = 0; i < Table.TableInfo.ColumnCount; i++) {
				SetValue(i, Table.GetValue(RowId.RowNumber, i));
			}
		}

		public void EvaluateAssignment(SqlAssignExpression assignExpression, IRequest context) {
			var colRef = (SqlReferenceExpression) assignExpression.ReferenceExpression;
			var valueExp = assignExpression.ValueExpression;
			var value = valueExp.EvaluateToConstant(context, VariableResolver);

			// Check the column name is within this row.
			var columnName = colRef.ReferenceName;
			int column = Table.FindColumn(columnName);

			if (column == -1)
				throw new ObjectNotFoundException(columnName,
					String.Format("Table '{0}' has none column named '{1}': cannot assign.", Table.TableInfo.TableName, columnName));

			SetValue(column, value);
		}

		#region RowInfo

		class RowInfo : IObjectInfo {
			public RowInfo(Row row) {
				Row = row;
			}

			private Row Row { get; set; }

			public DbObjectType ObjectType {
				get { return DbObjectType.Row; }
			}

			public ObjectName FullName {
				get { return new ObjectName(Row.Table.TableInfo.TableName, Row.RowId.ToString()); }
			}

			public string Owner {
				get { return null; }
			}
		}

		#endregion
	}
}