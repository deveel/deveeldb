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
using System.IO;

using Antlr4.Runtime;
using Antlr4.Runtime.Atn;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	public sealed class PlSqlCompiler : ISqlCompiler {
		private PlSqlParser plSqlParser;
		private PlSqlLexer lexer;
		private List<SqlCompileMessage> messages;

		public PlSqlCompiler() {
			MakeParser(String.Empty, message => messages.Add(message));
			messages = new List<SqlCompileMessage>(12);
		}

		~PlSqlCompiler() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (messages != null)
					messages.Clear();

				if (lexer != null) {
					lexer.Interpreter.ClearDFA();
					lexer.Reset();
					lexer.Interpreter = new LexerATNSimulator(new ATN(ATNType.Lexer, 2));
				}

				if (plSqlParser != null) {
					plSqlParser.Interpreter.ClearDFA();
					plSqlParser.Reset();
					plSqlParser.Interpreter = new ParserATNSimulator(new ATN(ATNType.Parser, 2));
				}
			}

			lexer = null;
			plSqlParser = null;
			messages = null;
		}

		private void SetInput(string inputString) {
			plSqlParser.SetInputStream(new BufferedTokenStream(new PlSqlLexer(new AntlrInputStream(inputString))));
			messages.Clear();
		}

		public SqlCompileResult Compile(SqlCompileContext context) {
			var result = new SqlCompileResult(context);

			try {
				SetInput(context.SourceText);

				// plSqlParser = MakeParser(context.SourceText, message => result.Messages.Add(message));
				var parseResult = plSqlParser.compilationUnit();

				if (parseResult == null)
					throw new InvalidOperationException();

				if (messages.Count > 0) {
					foreach (var message in messages) {
						result.Messages.Add(message);
					}
				}

				if (result.HasErrors)
					return result;

				var visitor = new SqlStatementVisitor();
				var statement = visitor.Visit(parseResult);

				if (statement is SequenceOfStatements) {
					var sequence = ((SequenceOfStatements) statement).Statements;
					foreach (var child in sequence) {
						result.Statements.Add(child);
					}
				} else {
					result.Statements.Add(statement);
				}
			} catch (Exception ex) {
				result.Messages.Add(new SqlCompileMessage(CompileMessageLevel.Error, ex.Message));
			}

			return result;
		}

		private class ErrorHandler : BaseErrorListener {
			private Action<SqlCompileMessage> receiveMessage;

			public ErrorHandler(Action<SqlCompileMessage> receiveMessage) {
				this.receiveMessage = receiveMessage;
			}
			 
			public override void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg,
				RecognitionException e) {
				if (receiveMessage != null)
					receiveMessage(new SqlCompileMessage(CompileMessageLevel.Error, msg, new SourceLocation(line, charPositionInLine)));
			}
			
		}


		private void MakeParser(string input, Action<SqlCompileMessage> messageReceiver) {
			using (var reader = new StringReader(input)) {
				var inputStream = new AntlrInputStream(reader);
				lexer = new PlSqlLexer(inputStream);

				var commonTokenStream = new CommonTokenStream(lexer);

				plSqlParser = new PlSqlParser(commonTokenStream);
				plSqlParser.RemoveErrorListeners();
				plSqlParser.AddErrorListener(new ErrorHandler(messageReceiver));
			}
		}

		public SqlExpression ParseExpression(string text) {
			SetInput(text);
			//var plSqlParser = MakeParser(text, null);
			var parseResult = plSqlParser.expressionUnit();

			var visitor = new SqlExpressionVisitor();
			var result = visitor.Visit(parseResult);
			return result;
		}

		public DataTypeInfo ParseDataType(string s) {
			SetInput(s);
			//var plSqlParser = MakeParser(s, null);
			var parseResult = plSqlParser.datatype();

			var visitor = new DataTypeVisitor();
			return visitor.Visit(parseResult);
		}
	}
}
