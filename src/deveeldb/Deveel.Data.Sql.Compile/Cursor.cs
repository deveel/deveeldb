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
using System.Text;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class Cursor {
		public static SqlStatement Close(PlSqlParser.CloseStatementContext context) {
			var cursorName = Name.Simple(context.cursor_name());

			return new CloseStatement(cursorName);
		}

		public static SqlStatement Open(PlSqlParser.OpenStatementContext context) {
			var cursorName = Name.Simple(context.cursor_name());
			SqlExpression[] args = null;

			if (context.expression_list() != null) {
				args = context.expression_list().expression().Select(Expression.Build).ToArray();
			}

			return new OpenStatement(cursorName, args);
		}

		public static SqlStatement Declare(PlSqlParser.CursorDeclarationContext context) {
			var cursorName = Name.Simple(context.cursor_name());
			var query = Subquery.Form(context.subquery());

			CursorParameter[] parameters = null;
			if (context.parameterSpec() != null) {
				parameters = context.parameterSpec().Select(Parameter.Form).ToArray();
			}

			return new DeclareCursorStatement(cursorName, parameters, query);
		}

		#region Parameter

		static class Parameter {
			public static CursorParameter Form(PlSqlParser.ParameterSpecContext context) {
				var paramName = Name.Simple(context.parameter_name());
				var type = SqlTypeParser.Parse(context.datatype());

				return new CursorParameter(paramName, type);
			}
		}

		#endregion
	}
}
