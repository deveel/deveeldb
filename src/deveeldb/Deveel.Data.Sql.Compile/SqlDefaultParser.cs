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
using System.Linq;

using Irony;
using Irony.Ast;
using Irony.Parsing;

namespace Deveel.Data.Sql.Compile {
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

			var startedOn = DateTimeOffset.UtcNow;

			try {
				var node = ParseNode(input, result.Errors);
				result.RootNode = node;
			} catch (Exception ex) {
				// TODO: form a better exception
				result.Errors.Add(new SqlParseError(ex.Message, 0, 0));
			} finally {
				result.ParseTime = DateTimeOffset.UtcNow.Subtract(startedOn);
			}

			return result;
		}

		private ISqlNode ParseNode(string sqlSource, ICollection<SqlParseError> errors) {
			if (!languageData.CanParse())
				throw new InvalidOperationException();

			var parser = new Parser(languageData);
			var tree = parser.Parse(sqlSource);
			if (tree.HasErrors()) {
				BuildErrors(errors, tree.ParserMessages);
				return null;
			}

			var astContext = new AstContext(languageData) {
				DefaultNodeType = typeof (SqlNode),
				DefaultIdentifierNodeType = typeof (IdentifierNode)
			};

			var astCompiler = new AstBuilder(astContext);
			astCompiler.BuildAst(tree);

			if (tree.HasErrors())
				BuildErrors(errors, tree.ParserMessages);

			var node = (ISqlNode) tree.Root.AstNode;
			if (node.NodeName == "root")
				node = node.ChildNodes.FirstOrDefault();

			return node;
		}

		private static void BuildErrors(ICollection<SqlParseError> errors, LogMessageList logMessages) {
			foreach (var logMessage in logMessages) {
				if (logMessage.Level == ErrorLevel.Error) {
					var line = logMessage.Location.Line;
					var column = logMessage.Location.Column;
					// TODO: build the message traversing the source ...

					errors.Add(new SqlParseError(logMessage.Message, line, column));
				}
			}
		}
	}
}
