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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// Defines the metadata properties of a column within a
	/// table of a database.
	/// </summary>
	/// <remarks>
	/// Columns have unique names within a table and a given
	/// <see cref="SqlType"/> that is used to define the type
	/// of data cells in the table will handle.
	/// </remarks>
	public sealed class ColumnInfo : IDbObjectInfo, ISqlFormattable {
		/// <summary>
		/// Constructs a new column with the given name and type.
		/// </summary>
		/// <param name="columnName">The name of the column, as case-sensitive and unique 
		/// within the table.</param>
		/// <param name="columnType">The <see cref="SqlType"/> that this column will handle.</param>
		/// <exception cref="ArgumentNullException">
		/// If either one of <paramref name="columnName"/> or <paramref name="columnType"/>
		/// is <c>null</c>.
		/// </exception>
		public ColumnInfo(string columnName, SqlType columnType) : this(columnName, columnType, null) {
		}

		/// <summary>
		/// Constructs a new column with the given name, given type and an optional default value.
		/// </summary>
		/// <param name="columnName">The name of the column, as case-sensitive and unique 
		/// within the table.</param>
		/// <param name="columnType">The <see cref="SqlType"/> that this column will handle.</param>
		/// <param name="defaultValue">An optional default value expression for the column</param>
		/// <exception cref="ArgumentNullException">
		/// If either one of <paramref name="columnName"/> or <paramref name="columnType"/>
		/// is <c>null</c>.
		/// </exception>
		public ColumnInfo(string columnName, SqlType columnType, SqlExpression defaultValue) {
			ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
			ColumnType = columnType ?? throw new ArgumentNullException(nameof(columnType));
			DefaultValue = defaultValue;
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Column;

		public ObjectName FullName => TableInfo != null
			? new ObjectName(TableInfo.TableName, ColumnName)
			: new ObjectName(ColumnName);

		/// <summary>
		/// Gets the name of the column.
		/// </summary>
		public string ColumnName { get; }

		public TableInfo TableInfo { get; internal set; }

		/// <summary>
		/// Gets the <see cref="SqlType"/> that cells within a table for this
		/// column will handle.
		/// </summary>
		/// <seealso cref="SqlType"/>
		public SqlType ColumnType { get; }

		public bool IsIndexable => ColumnType.IsIndexable;

		/// <summary>
		/// Gets or sets a <see cref="SqlExpression"/> used as a <c>DEFAULT</c>
		/// when a constraint for the column is to <c>SET DEFAULT</c>.
		/// </summary>
		/// <seealso cref="SqlExpression"/>
		public SqlExpression DefaultValue { get; set; }

		public bool HasDefault => DefaultValue != null;

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.Append(ColumnName);
			builder.Append(" ");
			ColumnType.AppendTo(builder);

			if (HasDefault) {
				builder.Append(" DEFAULT ");
				DefaultValue.AppendTo(builder);
			}
		}
	}
}