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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeclareCursorStatement : SqlStatement, IDeclarationStatement, IPlSqlStatement {
		public DeclareCursorStatement(string cursorName, SqlQueryExpression queryExpression) 
			: this(cursorName, null, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, SqlQueryExpression queryExpression) 
			: this(cursorName, parameters, CursorFlags.Insensitive, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, CursorFlags flags, SqlQueryExpression queryExpression) 
			: this(cursorName, null, flags, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, CursorFlags flags, SqlQueryExpression queryExpression) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
			Parameters = parameters;
			Flags = flags;
			QueryExpression = queryExpression;
		}

		private DeclareCursorStatement(SerializationInfo info, StreamingContext context) {
			CursorName = info.GetString("CursorName");
			QueryExpression = (SqlQueryExpression) info.GetValue("QueryExpression", typeof(SqlQueryExpression));
			Flags = (CursorFlags) info.GetInt32("Flags");

			var parameters = (CursorParameter[]) info.GetValue("Parameters", typeof(CursorParameter[]));

			if (parameters != null) {
				Parameters = new List<CursorParameter>(parameters);
			}
		}

		public string CursorName { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		public CursorFlags Flags { get; set; }

		public IEnumerable<CursorParameter> Parameters { get; set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("CursorName", CursorName);
			info.AddValue("QueryExpression", QueryExpression);
			info.AddValue("Flags", (int)Flags);

			if (Parameters != null) {
				var parameters = Parameters.ToArray();
				info.AddValue("Parameters", parameters);
			} else {
				info.AddValue("Parameters", new CursorParameter[0]);
			}
		}

		// TODO: assert access to the resources

		protected override void ExecuteStatement(ExecutionContext context) {
			var cursorInfo = new CursorInfo(CursorName, Flags, QueryExpression);
			if (Parameters != null) {
				foreach (var parameter in Parameters) {
					cursorInfo.Parameters.Add(parameter);
				}
			}

			var queryPlan = context.Request.Context.QueryPlanner().PlanQuery(new QueryInfo(context.Request, QueryExpression));
			var selectedTables = queryPlan.DiscoverTableNames();
			foreach (var tableName in selectedTables) {
				if (!context.User.CanSelectFromTable(tableName))
					throw new MissingPrivilegesException(context.User.Name, tableName, Privileges.Select);
			}


			context.Request.Context.DeclareCursor(cursorInfo, context.Request);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			// TODO: Flags ...

			builder.AppendFormat("CURSOR {0}", CursorName);

			if (Parameters != null) {
				var pars = Parameters.ToArray();

				builder.Append("(");

				for (int i = 0; i < pars.Length; i++) {
					pars[i].AppendTo(builder);

					if (i < pars.Length - 1)
						builder.Append(", ");
				}

				builder.Append(")");
			}

			builder.Append(" IS");
			builder.AppendLine();
			builder.Indent();

			QueryExpression.AppendTo(builder);

			builder.DeIndent();
		}
	}
}
