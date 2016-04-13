using System;
using System.Collections.Generic;
using System.IO;

using Antlr4.Runtime;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	public sealed class PlSqlCompiler : ISqlCompiler {
		//private PlSqlParser plSqlParser;
		//private List<SqlCompileMessage> messages; 

		public PlSqlCompiler() {
			//plSqlParser = MakeParser(String.Empty, message => messages.Add(message));
			//messages = new List<SqlCompileMessage>(12);
		}

		//~PlSqlCompiler() {
		//	Dispose(false);
		//}

		public void Dispose() {
			//Dispose(true);
			//GC.SuppressFinalize(this);
		}

		//private void Dispose(bool disposing) {
		//	if (disposing) {
		//		if (messages != null)
		//			messages.Clear();
		//	}

		//	//plSqlParser = null;
		//	messages = null;
		//}

		//private void SetInput(string inputString) {
		//	plSqlParser.SetInputStream(new BufferedTokenStream(new PlSqlLexer(new AntlrInputStream(inputString))));
		//	messages.Clear();
		//}

		public SqlCompileResult Compile(SqlCompileContext context) {
			var result = new SqlCompileResult(context);

			try {
				//SetInput(context.SourceText);

				var plSqlParser = MakeParser(context.SourceText, message => result.Messages.Add(message));
				var parseResult = plSqlParser.compilationUnit();

				//if (messages.Count > 0) {
				//	foreach (var message in messages) {
				//		result.Messages.Add(message);
				//	}
				//}

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


		private static PlSqlParser MakeParser(string input, Action<SqlCompileMessage> messageReceiver) {
			using (var reader = new StringReader(input)) {
				var inputStream = new AntlrInputStream(reader);
				var lexer = new PlSqlLexer(inputStream);

				var commonTokenStream = new CommonTokenStream(lexer);

				var parser = new PlSqlParser(commonTokenStream);
				parser.RemoveErrorListeners();
				parser.AddErrorListener(new ErrorHandler(messageReceiver));
				return parser;
			}
		}

		public SqlExpression ParseExpression(string text) {
			//SetInput(text);
			var plSqlParser = MakeParser(text, null);
			var parseResult = plSqlParser.expression_unit();

			var visitor = new SqlExpressionVisitor();
			var result = visitor.Visit(parseResult);
			return result;
		}

		public DataTypeInfo ParseDataType(string s) {
			// SetInput(s);
			var plSqlParser = MakeParser(s, null);
			var parseResult = plSqlParser.datatype();

			var visitor = new DataTypeVisitor();
			return visitor.Visit(parseResult);
		}
	}
}
