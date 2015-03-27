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
using System.Reflection;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public abstract class Statement {
		protected Statement() {
			StatementTree = new StatementTree(GetType());
		}

		protected StatementTree StatementTree { get; private set; }

		public SqlQuery Qeury { get; internal set; }

		internal void SetTree(StatementTree statementTree) {
			StatementTree = statementTree;
		}

		protected abstract PreparedStatement OnPrepare(IQueryContext context);

		public PreparedStatement Prepare(IQueryContext context) {
			var prepared = OnPrepare(context);

			prepared.Query = Qeury;

			// TODO: Make more checks here...
			return prepared;
		}

		public static IEnumerable<Statement> Parse(string sqlSource) {
			var compiler = new SqlCompiler();
			StatementSequenceNode sequence;

			try {
				sequence = compiler.CompileStatements(sqlSource);
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw;
			}

			var visitor = new StatementVisitor();
			visitor.VisitSequence(sequence);

			var trees = visitor.Statements;

			return trees.Select(x => x.CreateStatement()).ToList();
		}
	}
}