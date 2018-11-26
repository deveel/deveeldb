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

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlQueryExpressionItem : ISqlFormattable, ISqlExpressionPreparable<SqlQueryExpressionItem> {
		public SqlQueryExpressionItem(SqlExpression expression) 
			: this(expression, null) {
		}

		public SqlQueryExpressionItem(SqlExpression expression, string alias) {
			Expression = expression;
			Alias = alias;
		}

		static SqlQueryExpressionItem() {
			All = new SqlQueryExpressionItem(SqlExpression.Reference(new ObjectName("*")));
		}

		public SqlExpression Expression { get; }

		public string Alias { get; }

		public bool IsAliased => !String.IsNullOrWhiteSpace(Alias);

		public static SqlQueryExpressionItem All { get; }

		public bool IsAll => Expression is SqlReferenceExpression &&
		                     ((SqlReferenceExpression) Expression).ReferenceName.FullName == ObjectName.Glob.ToString();

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsAll) {
				builder.Append("*");
			} else {
				Expression.AppendTo(builder);

				if (IsAliased)
					builder.AppendFormat(" AS {0}", Alias);
			}
		}

		SqlQueryExpressionItem ISqlExpressionPreparable<SqlQueryExpressionItem>.Prepare(ISqlExpressionPreparer preparer) {
			var exp = Expression.Prepare(preparer);

			return new SqlQueryExpressionItem(exp, Alias);
		}
	}
}