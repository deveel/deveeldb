// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlColumnAssignment : IPreparable {
		public SqlColumnAssignment(string columnName, SqlExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			ColumnName = columnName;
			Expression = expression;
		}

		public string ColumnName { get; private set; }

		public SqlExpression Expression { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var expression = Expression;
			if (expression != null)
				expression = expression.Prepare(preparer);

			return new SqlColumnAssignment(ColumnName, expression);
		}
	}
}
