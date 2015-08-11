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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	public sealed class SqlDefaultCompiler : ISqlCompiler {
		public SqlDefaultCompiler() {
			StatementSerializerResolver =new StatementSerializerProvider();
		}

		public IObjectSerializerResolver StatementSerializerResolver { get; private set; }

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
