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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class ViewStatements {
		public static CreateViewStatement Create(PlSqlParser.CreateViewStatementContext context) {
			var orReplace = context.OR() != null && context.REPLACE() != null;

			var viewName = Name.Object(context.objectName());
			var query = (SqlQueryExpression) Expression.Build(context.subquery());

			string[] columnNames = null;
			if (context.columnList() != null) {
				columnNames = context.columnList().columnName().Select(Name.Simple).ToArray();
			}

			return new CreateViewStatement(viewName, columnNames, query) {
				ReplaceIfExists = orReplace
			};
		}

		public static SqlStatement Drop(PlSqlParser.DropViewStatementContext context) {
			var names = context.objectName().Select(Name.Object).ToArray();
			var ifExists = context.IF() != null && context.EXISTS() != null;

			if (names.Length == 1)
				return new DropViewStatement(names[0], ifExists);

			var sequence = new SequenceOfStatements();
			foreach (var name in names) {
				sequence.Statements.Add(new DropViewStatement(name, ifExists));
			}

			return sequence;
		}
	}
}
