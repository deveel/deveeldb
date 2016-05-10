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

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class DeleteBuilder {
		public static SqlStatement Build(PlSqlParser.DeleteStatementContext context) {
			var tableName = Name.Object(context.objectName());
			var whereClause = WhereClause.Form(context.whereClause());

			if (whereClause.CurrentOf != null)
				return new DeleteCurrentStatement(tableName, whereClause.CurrentOf);

			var statement = new DeleteStatement(tableName, whereClause.Expression);

			if (context.deleteLimit() != null) {
				var limit = Number.PositiveInteger(context.deleteLimit().numeric());
				if (limit == null)
					throw new ParseCanceledException("Invalid delete limit.");

				statement.Limit = limit.Value;
			}

			return statement;
		}
	}
}
