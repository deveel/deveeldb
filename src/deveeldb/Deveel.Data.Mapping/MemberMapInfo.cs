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
using System.Reflection;

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMapInfo {
		internal MemberMapInfo(MemberInfo member, string columnName, SqlType columnType, bool nullable, ConstraintMapInfo[] constraints) {
			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
			IsNullable = nullable;
			Constraints = constraints;

			if (member is PropertyInfo) {
				MemberType = ((PropertyInfo)member).PropertyType;
			} else if (member is FieldInfo) {
				MemberType = ((FieldInfo)member).FieldType;
			} else {
				throw new ArgumentException(String.Format("Member of type '{0}' is not permitted.", member.GetType()));
			}
		}

		public MemberInfo Member { get; private set; }

		private Type MemberType { get; set; }

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public bool IsNullable { get; private set; }

		public object Default { get; private set; }

		public bool DefaultIsExpression { get; private set; }

		internal ConstraintMapInfo[] Constraints { get; private set; }

		internal void SetDefault(object value, bool isExpression) {
			if (isExpression &&
				(!(value is string) &&
				(!(value is SqlExpression))))
				throw new ArgumentException("Cannot set an expression value that is not a string or an Expression.");

			Default = value;
			DefaultIsExpression = isExpression;
		}

		internal ColumnInfo AsColumnInfo() {
			var columnInfo = new ColumnInfo(ColumnName, ColumnType) {
				IsNotNull = !IsNullable
			};

			if (Default != null) {
				SqlExpression defaultExpression;
				if (DefaultIsExpression) {
					defaultExpression = SqlExpression.Parse((string)Default);
				} else {
					defaultExpression = SqlExpression.Constant(Field.Create(Default));
				}

				columnInfo.DefaultExpression = defaultExpression;
			}

			return columnInfo;
		}

		internal SqlTableColumn AsTableColumn() {
			var column = new SqlTableColumn(ColumnName, ColumnType) {
				IsNotNull = !IsNullable
			};

			if (Default != null) {
				SqlExpression defaultExpression;
				if (DefaultIsExpression) {
					defaultExpression = SqlExpression.Parse((string)Default);
				} else {
					defaultExpression = SqlExpression.Constant(Field.Create(Default));
				}

				column.DefaultExpression = defaultExpression;
			}

			return column;
		}

		internal void SetValue(Row row, object obj) {
			var colIndex = row.Table.TableInfo.IndexOfColumn(ColumnName);
			if (colIndex < 0)
				throw new InvalidOperationException(String.Format("The source table '{0}' has no column named '{1}'.",
					row.Table.TableInfo.TableName, ColumnName));

			var value = row.GetValue(colIndex);
			if (Field.IsNullField(value)) {
				if (!IsNullable)
					throw new InvalidOperationException(String.Format("Cannot set NULL to the non-nullable field '{0}' of {1}.",
						Member.Name, Member.DeclaringType));
			}

			var memberValue = value.ConvertTo(MemberType);

			if (Member is PropertyInfo) {
				((PropertyInfo)Member).SetValue(obj, memberValue, null);
			} else if (Member is FieldInfo) {
				((FieldInfo)Member).SetValue(obj, memberValue);
			}
		}
	}
}