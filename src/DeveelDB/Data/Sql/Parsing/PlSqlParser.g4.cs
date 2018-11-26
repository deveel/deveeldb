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
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Antlr4.Runtime;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parsing {
	partial class PlSqlParser {
		private static PlSqlParser MakeParser(string input, Action<SqlParseMessage> messageReceiver) {
			using (var reader = new StringReader(input)) {
				var inputStream = new AntlrInputStream(reader);
				var lexer = new PlSqlLexer(inputStream);

				var commonTokenStream = new CommonTokenStream(lexer);

				var plSqlParser = new PlSqlParser(commonTokenStream);
				plSqlParser.RemoveErrorListeners();
				plSqlParser.AddErrorListener(new PlSqlParserErrorHandler(messageReceiver));

				return plSqlParser;
			}
		}

		internal static SqlExpressionParseResult ParseExpression(IContext context, string text) {
			var messages = new List<SqlParseMessage>();
			var plSqlParser = MakeParser(text, message => messages.Add(message));

			//var plSqlParser = MakeParser(text, null);
			var parseResult = plSqlParser.expressionUnit();

			var visitor = new SqlExpressionVisitor(context);
			var result = visitor.Visit(parseResult);

			var errors = messages.Where(x => x.Level == SqlParseMessageLevel.Error).Select(x => x.Message).ToArray();
			if (errors.Length > 0)
				return SqlExpressionParseResult.Fail(errors);

			return SqlExpressionParseResult.Success(result);
		}

		internal static SqlTypeResolveInfo ParseType(string s) {
			var plSqlParser = MakeParser(s, message => { });

			//var plSqlParser = MakeParser(s, null);
			var parseResult = plSqlParser.datatype();

			return  SqlTypeParser.GetResolveInfo(parseResult);
		}

		#region ErrorHandler

		private class PlSqlParserErrorHandler : BaseErrorListener {
			private Action<SqlParseMessage> receiveMessage;

			public PlSqlParserErrorHandler(Action<SqlParseMessage> receiveMessage) {
				this.receiveMessage = receiveMessage;
			}

			public override void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line,
				int charPositionInLine, string msg, RecognitionException e) {
				receiveMessage?.Invoke(new SqlParseMessage(msg, SqlParseMessageLevel.Error,
					new LocationInfo(line, charPositionInLine)));
			}
		}

		#endregion
	}
}