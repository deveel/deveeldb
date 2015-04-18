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
		public SqlQuery SourceQuery { get; private set; }

		public bool IsFromQuery { get; private set; }

		internal void SetSource(SqlQuery query) {
			SourceQuery = query;
			IsFromQuery = true;
		}

		public abstract StatementType StatementType { get; }

		protected abstract PreparedStatement PrepareStatement(IQueryContext context);

		public PreparedStatement Prepare(IQueryContext context) {
			PreparedStatement prepared;

			try {
				prepared = PrepareStatement(context);

				if (prepared == null)
					throw new InvalidOperationException("Preparation was invalid.");

				prepared.Source = this;
			} catch (Exception ex) {
				// TODO: throw a specialized exception
				throw;
			}

			return prepared;
		}

		public static IEnumerable<Statement> Parse(string sqlSource) {
			var compiler = SqlParsers.Default;
			SequenceOfStatementsNode sequence;

			SqlParseResult result;

			try {
				result = compiler.Parse(sqlSource);
				if (result.HasErrors)
					throw new SqlParseException();
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw;
			}

			var builder = new StatementBuilder();
			var trees = builder.Build(result.RootNode, sqlSource);

			return trees.ToList();
		}
	}
}