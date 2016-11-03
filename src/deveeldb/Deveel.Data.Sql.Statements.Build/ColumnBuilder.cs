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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements.Build {
	class ColumnBuilder : IColumnBuilder {
		private string columnName;
		private SqlType columnType;
		private bool notNull;
		private string indexType;
		private bool identity;
		private SqlExpression defaultExpression;
		private ColumnConstraintInfo constraintInfo;

		public IColumnBuilder Named(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			columnName = name;
			return this;
		}

		public IColumnBuilder OfType(SqlType type) {
			if (type == null)
				throw new ArgumentNullException("type");

			columnType = type;
			return this;
		}

		public IColumnBuilder NotNull(bool value = true) {
			notNull = value;
			return this;
		}

		public IColumnBuilder WithIndexType(string value) {
			indexType = value;
			return this;
		}

		public IColumnBuilder WithDefault(SqlExpression expression) {
			defaultExpression = expression;
			return this;
		}

		public IColumnBuilder Identity(bool value = true) {
			identity = value;
			return this;
		}

		public IColumnBuilder WithConstraint(ColumnConstraintInfo constraint) {
			constraintInfo = constraint;
			return this;
		}

		public SqlTableColumn Build(out ColumnConstraintInfo constraint) {
			if (String.IsNullOrEmpty(columnName))
				throw new InvalidOperationException("The name for the column is required");
			if (columnType == null)
				throw new InvalidOperationException("The type for the column is required");

			constraint = constraintInfo;

			return new SqlTableColumn(columnName, columnType) {
				IsNotNull = notNull,
				DefaultExpression = defaultExpression,
				IndexType = indexType,
				IsIdentity = identity
			};
		}
	}
}
