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

using Antlr4.Runtime.Misc;

using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	static class TableColumn {
		public static SqlTableColumn Form(PlSqlParser.TableColumnContext context, List<ColumnConstraint> constraints) {

			var columnName = Name.Simple(context.columnName());
			var columnType = SqlTypeParser.Parse(context.datatype());

			if (columnType == null)
				throw new ParseCanceledException("No type was found for table.");

			SqlExpression defaultExpression = null;
			bool identity = false;
			bool nullable = true;
			string indexType = null;

			if (context.IDENTITY() != null) {
				if (!(columnType is NumericType))
					throw new InvalidOperationException("Cannot have an identity column that has not a numeric type");

				identity = true;
			} else {
				var columnConstraints = context.columnConstraint();
				if (columnConstraints != null &&
				    columnConstraints.Length > 0) {
					foreach (var constraintContext in columnConstraints) {
						if (constraintContext.PRIMARY() != null) {
							constraints.Add(new ColumnConstraint {
								ColumnName = columnName,
								Type = ConstraintType.PrimaryKey
							});
						} else if (constraintContext.UNIQUE() != null) {
							constraints.Add(new ColumnConstraint {
								Type = ConstraintType.Unique,
								ColumnName = columnName
							});
						} else if (constraintContext.NOT() != null &&
						           constraintContext.NULL() != null) {
							nullable = false;
						}
					}
				}

				if (context.defaultValuePart() != null) {
					defaultExpression = Expression.Build(context.defaultValuePart().expression());
				}

				if (context.columnIndex() != null) {
					var columnIndex = context.columnIndex();
					if (columnIndex.BLIST() != null) {
						indexType = DefaultIndexTypes.InsertSearch;
					} else if (columnIndex.NONE() != null) {
						indexType = DefaultIndexTypes.BlindSearch;
					} else if (columnIndex.id() != null) {
						indexType = Name.Simple(columnIndex.id());
					} else if (columnIndex.CHAR_STRING() != null) {
						indexType = InputString.AsNotQuoted(columnIndex.CHAR_STRING());
					}
				}
			}

			return new SqlTableColumn(columnName, columnType) {
				IsNotNull = !nullable,
				IsIdentity = identity,
				DefaultExpression = defaultExpression,
				IndexType = indexType
			};
		}
	}
}