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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public abstract class Statement {
		protected Statement() {
			StatementTree = new StatementTree(GetType());
		}

		protected StatementTree StatementTree { get; private set; }

		public SqlQuery SourceQuery { get; private set; }

		public bool IsFromQuery { get; private set; }

		internal void SetSource(StatementTree statementTree, SqlQuery query) {
			StatementTree = statementTree;
			SourceQuery = query;
			IsFromQuery = true;
		}

		protected abstract PreparedStatement OnPrepare(IQueryContext context);

		protected T GetValue<T>(string key) {
			return StatementTree.GetValue<T>(key);
		}

		protected IList<T> GetList<T>(string key) {
			return StatementTree.GetList<T>(key);
		} 

		protected void SetValue(string key, object value) {
			StatementTree.SetValue(key, value);
		}

		public PreparedStatement Prepare(IQueryContext context) {
			var prepared = OnPrepare(context);

			prepared.Query = SourceQuery;

			// TODO: Make more checks here...
			return prepared;
		}

		public static IEnumerable<Statement> Parse(string sqlSource) {
			var compiler = new SqlCompiler();
			SequenceOfStatementsNode sequence;

			try {
				sequence = compiler.CompileStatements(sqlSource);
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw;
			}

			var builder = new StatementBuilder();
			var trees = builder.Build(sequence, sqlSource);

			return trees.Select(x => x.CreateStatement()).ToList();
		}
	}
}