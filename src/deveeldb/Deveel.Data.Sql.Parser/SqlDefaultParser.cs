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
using System.Diagnostics;
using System.Linq;
using System.Text;

using Irony;
using Irony.Ast;
using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	class SqlDefaultParser : ISqlParser {
		private LanguageData languageData;
		private Irony.Parsing.Parser parser;

		public SqlDefaultParser(SqlGrammarBase grammar) {
			languageData = new LanguageData(grammar);
			parser = new Irony.Parsing.Parser(languageData);

			if (!languageData.CanParse())
				throw new InvalidOperationException();
		}

		private void Dispose(bool disposing) {
			parser = null;
			languageData = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public string Dialect {
			get { return ((SqlGrammarBase) languageData.Grammar).Dialect; }
		}

		public SqlParseResult Parse(string input) {
			var result = new SqlParseResult(Dialect);

			var timer = new Timer();

			try {
				long time;
				var node = ParseNode(input, result.Errors, out time);
				result.RootNode = node;
			} catch(SqlParseException ex) {
				result.Errors.Add(new SqlParseError(ex.Message, ex.Level, ex.Line, ex.Column));
		} catch (Exception ex) {
				result.Errors.Add(SqlParseError.Unhandled(ex));
			} finally {
				timer.Dispose();
				result.ParseTime = timer.Elapsed;
			}

			return result;
		}

		private ISqlNode ParseNode(string sqlSource, ICollection<SqlParseError> errors, out long parseTime) {
			var tree = parser.Parse(sqlSource);
			parseTime = tree.ParseTimeMilliseconds;

			if (tree.Status == ParseTreeStatus.Error) {
				BuildErrors(tree, errors, tree.ParserMessages);
				return null;
			}

			var astContext = new AstContext(languageData) {
				DefaultNodeType = typeof (SqlNode),
				DefaultIdentifierNodeType = typeof (IdentifierNode)
			};

			var astCompiler = new AstBuilder(astContext);
			astCompiler.BuildAst(tree);

			if (tree.HasErrors())
				BuildErrors(tree, errors, tree.ParserMessages);

			var node = (ISqlNode) tree.Root.AstNode;
			if (node.NodeName == "root")
				node = node.ChildNodes.FirstOrDefault();

			return node;
		}

		private static void BuildErrors(ParseTree tree, ICollection<SqlParseError> errors, LogMessageList logMessages) {
			foreach (var logMessage in logMessages) {
				if (logMessage.Level == ErrorLevel.Error) {
					var line = logMessage.Location.Line;
					var column = logMessage.Location.Column;
					var locationMessage = FormInfoMessage(tree, line, column);
					string[] expected = null;
					if (logMessage.ParserState.ReportedExpectedSet != null)
						expected = logMessage.ParserState.ReportedExpectedSet.ToArray();

					var infoMessage = String.Format("A parse error occurred near '{0}' in the source", locationMessage);
					if (expected != null && expected.Length > 0)
						infoMessage = String.Format("{0}. Expected {1}", infoMessage, String.Join(", ", expected));
					
					errors.Add(new SqlParseError(infoMessage, line, column));
				}
			}
		}

		private static string FormInfoMessage(ParseTree tree, int line, int column) {
			const int tokensBeforeCount = 10;
			const int tokensAfterCount = 10;

			var tokensBefore = FindTokensTo(tree, line, column).Reverse().ToList();
			var tokensAfter = FindTokensFrom(tree, line, column);

			var countTokensBefore = System.Math.Min(tokensBefore.Count, tokensBeforeCount);
			var countTokensAfter = System.Math.Min(tokensAfterCount, tokensAfter.Count);

			var takeTokensBefore = tokensBefore.Take(countTokensBefore).Reverse();
			var takeTokensAfter = tokensAfter.Take(countTokensAfter);

			var sb = new StringBuilder();
			foreach (var token in takeTokensBefore) {
				sb.Append(token.Text);
				sb.Append(" ");
			}

			foreach (var token in takeTokensAfter) {
				sb.Append(token.Text);
				sb.Append(" ");
			}

			return sb.ToString();
		}

		private static IList<Irony.Parsing.Token> FindTokensFrom(ParseTree tree, int line, int column) {
			var tokens = tree.Tokens;
			bool startCollect = false;

			var result = new List<Irony.Parsing.Token>();
			foreach (var token in tokens) {
				if (token.Location.Line == line &&
				    token.Location.Column == column) {
					startCollect = true;
				}

				if (startCollect)
					result.Add(token);
			}

			return result.ToList();
		}

		private static IList<Irony.Parsing.Token> FindTokensTo(ParseTree tree, int line, int column) {
			var tokens = tree.Tokens;

			var result = new List<Irony.Parsing.Token>();
			foreach (var token in tokens) {
				if (token.Location.Line == line &&
				    token.Location.Column == column)
					break;

				result.Add(token);
			}

			return result.ToList();
		}

#region Timer

		class Timer : IDisposable {
#if PCL
			private readonly DateTimeOffset startTime;
#else
			private Stopwatch stopwatch;
#endif

			public Timer() {
#if PCL
				startTime = DateTimeOffset.UtcNow;
#else
				stopwatch = new Stopwatch();
				stopwatch.Start();
#endif
			}

			public TimeSpan Elapsed {
				get {
#if PCL
					return DateTimeOffset.UtcNow.Subtract(startTime);
#else
					return stopwatch.Elapsed;
#endif
				}
			}

			public void Dispose() {
#if !PCL
				if (stopwatch != null)
					stopwatch.Stop();
#endif
			}
		}

#endregion
	}
}
