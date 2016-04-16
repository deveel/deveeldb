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
			if (context.parameter_spec() != null) {
				parameters = context.parameter_spec().Select(Parameter.Form).ToArray();
			}

			return new DeclareCursorStatement(cursorName, parameters, query);
		}

		#region Parameter

		static class Parameter {
			public static CursorParameter Form(PlSqlParser.Parameter_specContext context) {
				var paramName = Name.Simple(context.parameter_name());
				var type = SqlTypeParser.Parse(context.datatype());

				return new CursorParameter(paramName, type);
			}
		}

		#endregion
	}
}
