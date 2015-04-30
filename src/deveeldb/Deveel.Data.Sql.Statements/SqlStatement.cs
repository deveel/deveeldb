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
	public abstract class SqlStatement {
		public SqlQuery SourceQuery { get; private set; }

		public bool IsFromQuery { get; private set; }

		internal void SetSource(SqlQuery query) {
			SourceQuery = query;
			IsFromQuery = true;
		}

		public abstract StatementType StatementType { get; }

		protected abstract SqlPreparedStatement PrepareStatement(IQueryContext context);

		public SqlPreparedStatement Prepare(IQueryContext context) {
			SqlPreparedStatement prepared;

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

		public ITable Evaluate(IQueryContext context) {
			SqlPreparedStatement prepared;

			try {
				prepared = Prepare(context);
			} catch (Exception ex) {
				throw new InvalidOperationException("Unable to prepare the statement for execution.");
			}

			return prepared.Evaluate(context);
		}

		public static IEnumerable<SqlStatement> Parse(string sqlSource) {
			var compiler = SqlParsers.Default;

			try {
				var result = compiler.Parse(sqlSource);
				if (result.HasErrors)
					throw new SqlParseException();

				var builder = new StatementBuilder();
				return builder.Build(result.RootNode, sqlSource);
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw;
			}
		}
	}
}