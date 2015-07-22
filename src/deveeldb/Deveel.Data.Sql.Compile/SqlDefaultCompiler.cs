using System;

using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql.Compile {
	public sealed class SqlDefaultCompiler : ISqlCompiler {
		public SqlCompileResult Compile(SqlCompileContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			var compiler = SqlParsers.Default;
			var compileResult = new SqlCompileResult(context);

			try {
				var sqlSource = context.SourceText;
				var result = compiler.Parse(sqlSource);

				foreach (var error in result.Errors) {
					var location = new SourceLocation(error.Line, error.Column);
					compileResult.Messages.Add(new SqlCompileMessage(CompileMessageLevel.Error, error.Message, location));
				}

				var builder = new StatementBuilder();
				var statements = builder.Build(result.RootNode, sqlSource);

				foreach (var statement in statements) {
					compileResult.Statements.Add(statement);
				}
			} catch (SqlParseException ex) {
				compileResult.Messages.Add(new SqlCompileMessage(CompileMessageLevel.Error, ex.Message));
			} catch (Exception ex) {
				compileResult.Messages.Add(new SqlCompileMessage(CompileMessageLevel.Error, ex.Message));
			}

			return compileResult;
		}
	}
}
