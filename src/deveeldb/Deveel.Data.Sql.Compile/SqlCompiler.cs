// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Types;

using Irony.Ast;
using Irony.Parsing;

namespace Deveel.Data.Sql.Compile {
	public sealed class SqlCompiler {
		public SqlCompiler() 
			: this(true) {
		}

		public SqlCompiler(bool ignoreCase) {
			IgnoreCase = ignoreCase;
		}

		public bool IgnoreCase { get; set; }

		public TNode Compile<TNode>(string sqlSource) where TNode : ISqlNode {
			var grammar = new SqlGrammar(IgnoreCase);
			var languageData = new LanguageData(grammar);

			if (!languageData.CanParse())
				throw new InvalidOperationException();

			var parser = new Parser(languageData);
			var result = parser.Parse(sqlSource);
			if (result.HasErrors())
				throw new SqlParseException();

			var astContext = new AstContext(languageData);
			astContext.DefaultNodeType = typeof (SqlNode);
			var astCompiler = new AstBuilder(astContext);
			astCompiler.BuildAst(result);

			if (result.HasErrors())
				throw new SqlParseException();

			return (TNode) result.Root.AstNode;
		}

		public DataTypeNode CompileDataType(string s) {
			var grammar = new SqlGrammar(IgnoreCase);
			grammar.SetRootToDataType();

			var languageData = new LanguageData(grammar);

			if (!languageData.CanParse())
				throw new InvalidOperationException();

			var parser = new Parser(languageData);
			var result = parser.Parse(s);
			if (result.HasErrors())
				throw new SqlParseException();

			var astContext = new AstContext(languageData);
			astContext.DefaultNodeType = typeof(SqlNode);
			astContext.DefaultIdentifierNodeType = typeof (IdentifierNode);
			var astCompiler = new AstBuilder(astContext);
			astCompiler.BuildAst(result);

			if (result.HasErrors())
				throw new SqlParseException();

			return (DataTypeNode)result.Root.AstNode;			
		}

		public IExpressionNode CompileExpression(string s) {
			var grammar = new SqlGrammar(IgnoreCase);
			grammar.SetRootToExpression();

			var languageData = new LanguageData(grammar);

			if (!languageData.CanParse())
				throw new InvalidOperationException();

			var parser = new Parser(languageData);
			var result = parser.Parse(s);
			if (result.HasErrors())
				throw new SqlParseException();

			var astContext = new AstContext(languageData);
			astContext.DefaultNodeType = typeof(SqlNode);
			astContext.DefaultIdentifierNodeType = typeof (IdentifierNode);
			var astCompiler = new AstBuilder(astContext);
			astCompiler.BuildAst(result);

			if (result.HasErrors())
				throw new SqlParseException();

			return (IExpressionNode)result.Root.AstNode;
		}
	}
}