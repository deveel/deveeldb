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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

using Irony;
using Irony.Ast;
using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	class SqlDefaultParser : ISqlParser {
		private readonly LanguageData languageData;

		public SqlDefaultParser(SqlGrammarBase grammar) {
			languageData = new LanguageData(grammar);
		}

		public void Dispose() {
		}

		public string Dialect {
			get { return ((SqlGrammarBase) languageData.Grammar).Dialect; }
		}

		public SqlParseResult Parse(string input) {
			var result = new SqlParseResult(Dialect);

			var stopwatch = new Stopwatch();
			stopwatch.Start();

			try {
				long time;
				var node = ParseNode(input, result.Errors, out time);
				result.RootNode = node;
			} catch (Exception ex) {
				// TODO: form a better exception
				result.Errors.Add(new SqlParseError(ex.Message, 0, 0));
			} finally {
				stopwatch.Stop();
				result.ParseTime = stopwatch.Elapsed;
			}

			return result;
		}

		private ISqlNode ParseNode(string sqlSource, ICollection<SqlParseError> errors, out long parseTime) {
			if (!languageData.CanParse())
				throw new InvalidOperationException();

			var parser = new Irony.Parsing.Parser(languageData);
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
					var expected = logMessage.ParserState.ReportedExpectedSet.ToArray();
					var infoMessage = String.Format("A parse error occurred near '{0}' in the source", locationMessage);
					if (expected.Length > 0)
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
	}
}
