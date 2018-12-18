// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public sealed class Row : IDbObject {
		private SqlObject[] values;

		private Row(ITable table, long number, bool attached) {
			Table = table ?? throw new ArgumentNullException(nameof(table));
			Number = number;
			IsAttached = attached;
		}

		public Row(ITable table, long number)
			: this(table, number, true) {
		}

		public Row(ITable table)
			: this(table, -1, false) {
		}

		public long Number { get; private set; }

		public bool IsAttached { get; private set; }

		public ITable Table { get; }

		public SqlObject this[int column] {
			get => GetValue(column);
			set => SetValue(column, value);
		}

		public SqlObject this[string columnName] {
			get => GetValue(columnName);
			set => SetValue(columnName, value);
		}

		public IReferenceResolver ReferenceResolver => new RowReferenceResolver(this);

		IDbObjectInfo IDbObject.ObjectInfo => new RowInfo(this);

		private int ColumnOffset(string columnName) {
			if (!IsAttached)
				throw new InvalidOperationException("The row is not sourced by any table");

			var index = Table.TableInfo.Columns.IndexOf(columnName);
			if (index == -1)
				throw new ArgumentException($"Table '{Table.TableInfo.TableName}' has no column named '{columnName}'.");

			return index;
		}

		public void Attach(long number) {
			if (number < 0)
				throw new ArgumentException("Row number cannot be smaller than zero");

			Number = number;
			IsAttached = true;
		}

		public async Task<SqlObject> GetValueAsync(int column) {
			if (column < 0 || column > Table.TableInfo.Columns.Count)
				throw new ArgumentOutOfRangeException(nameof(column), column, $"Column is out of range of the columns of table '{Table.TableInfo.TableName}'");

			if (IsAttached && values == null) {
				values = new SqlObject[Table.TableInfo.Columns.Count];

				for (int i = 0; i < Table.TableInfo.Columns.Count; i++) {
					values[i] = await Table.GetValueAsync(Number, i);
				}
			}

			if (values == null)
				return SqlObject.Unknown;

			return values[column];
		}

		public Task<SqlObject> GetValueAsync(string columnName) {
			return GetValueAsync(ColumnOffset(columnName));
		}

		public SqlObject GetValue(int column)
			=> GetValueAsync(column).Result;

		public SqlObject GetValue(string columnName)
			=> GetValueAsync(columnName).Result;

		public Task SetValueAsync(int column, SqlObject value) {
			if (column < 0 || column > Table.TableInfo.Columns.Count)
				throw new ArgumentOutOfRangeException(nameof(column), column,
					$"Column is out of range of the columns of table '{Table.TableInfo.TableName}'");

			if (IsAttached)
				throw new InvalidOperationException("The row is attached to a table: setting values is not permitted in this context");

			if (values == null)
				values = new SqlObject[Table.TableInfo.Columns.Count];

			var columnType = Table.TableInfo.Columns[column].ColumnType;

			if (!columnType.Equals(value.Type)) {
				if (!value.CanCastTo(columnType))
					throw new ArgumentException($"The value cannot be casted to type '{columnType}'.");

				value = value.CastTo(columnType);
			}

			values[column] = value;

			return Task.CompletedTask;
		}

		public Task SetValueAsync(string columnName, SqlObject value)
			=> SetValueAsync(ColumnOffset(columnName), value);

		public void SetValue(int column, SqlObject value)
			=> SetValueAsync(column, value).Wait();

		public void SetValue(string columnName, SqlObject value)
			=> SetValueAsync(columnName, value).Wait();

		public async Task SetDefaultAsync(IContext context, int column) {
			if (column < 0 || column > Table.TableInfo.Columns.Count)
				throw new ArgumentOutOfRangeException(nameof(column), column,
					$"Column is out of range of the columns of table '{Table.TableInfo.TableName}'");

			SqlObject value;

			using (var rowContext = context.CreateQuery(context.GroupResolver(), ReferenceResolver)) {
				var columnInfo = Table.TableInfo.Columns[column];

				if (columnInfo.HasDefault) {
					var valueExpression = await columnInfo.DefaultValue.ReduceAsync(rowContext);

					if (valueExpression.ExpressionType != SqlExpressionType.Constant)
						throw new SqlExpressionException(
							$"The default expression of the column '{columnInfo.FullName}' does not resolve to a constant");

					value = ((SqlConstantExpression) valueExpression).Value;
				}
				else {
					value = SqlObject.Null;
				}
			}

			await SetValueAsync(column, value);
		}

		public async Task SetDefaultAsync(IContext context) {
			if (values == null)
				values = new SqlObject[Table.TableInfo.Columns.Count];

			for (int i = values.Length - 1; i >= 0; i--) {
				if (values[i] == null)
					await SetDefaultAsync(context, i);
			}
		}

		#region RowInfo

		class RowInfo : IDbObjectInfo {
			private readonly Row row;

			public RowInfo(Row row) {
				this.row = row;
			}

			public DbObjectType ObjectType => DbObjectType.Row;

			public ObjectName FullName => row.IsAttached
				? new ObjectName(row.Table.TableInfo.TableName, row.Number.ToString())
				: new ObjectName("ND");
		}

		#endregion

		#region RowReferenceResolver

		class RowReferenceResolver : IReferenceResolver {
			private readonly Row row;

			public RowReferenceResolver(Row row) {
				this.row = row;
			}

			public Task<SqlObject> ResolveReferenceAsync(ObjectName referenceName) {
				var index = row.Table.TableInfo.Columns.IndexOf(referenceName);

				if (index == -1)
					throw new SqlExpressionException(
						$"The reference '{referenceName}' was not found in the context of the table '{row.Table.TableInfo.TableName}'.");

				return row.GetValueAsync(index);
			}

			public SqlType ResolveType(ObjectName referenceName) {
				var index = row.Table.TableInfo.Columns.IndexOf(referenceName);

				if (index == -1)
					throw new SqlExpressionException(
						$"The reference '{referenceName}' was not found in the context of the table '{row.Table.TableInfo.TableName}'.");

				return row.Table.TableInfo.Columns[index].ColumnType;
			}
		}

		#endregion
	}
}