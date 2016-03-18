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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ShowStatement : SqlStatement {
		public ShowStatement(ShowTarget target) 
			: this(target, null) {
		}

		public ShowStatement(ShowTarget target, ObjectName tableName) {
			Target = target;
			TableName = tableName;
		}

		public ShowTarget Target { get; private set; }

		public ObjectName TableName { get; set; }
	

		protected override SqlStatement PrepareStatement(IRequest context) {
			ObjectName tableName = null;

			if (Target == ShowTarget.Table &&
			    TableName != null) {
				tableName = context.Access.ResolveTableName(TableName);
			}

			if (Target == ShowTarget.Schema)
				return ShowSchema(context.Context);
			if (Target == ShowTarget.SchemaTables)
				return ShowSchemaTables(context.Query.CurrentSchema(), context.Context);

			throw new StatementException(String.Format("The SHOW target {0} is not supported.", Target));
		}

		private static SqlStatement Show(IContext context, string sql, params SortColumn[] orderBy) {
			var query = (SqlQueryExpression)SqlExpression.Parse(sql, context);
			var select = new SelectStatement(query, orderBy);
			return new Prepared(select);
		}

		private SqlStatement ShowSchema(IContext systemContext) {
			var sql = "SELECT \"name\" AS \"schema_name\", " +
			          "       \"type\", " +
			          "       \"other\" AS \"notes\" " +
			          "    FROM " + InformationSchema.ThisUserSchemaInfoViewName;

			return Show(systemContext, sql, new SortColumn("schema_name"));
		}

		private SqlStatement ShowSchemaTables(string schema, IContext context) {
			var sql = "  SELECT \"Tables.TABLE_NAME\" AS \"table_name\", " +
			          "         I_PRIVILEGE_STRING(\"agg_priv_bit\") AS \"user_privs\", " +
			          "         \"Tables.TABLE_TYPE\" as \"table_type\" " +
			          "    FROM " + InformationSchema.Tables + ", " +
			          "         ( SELECT AGGOR(\"priv_bit\") agg_priv_bit, " +
			          "                  \"object\", \"param\" " +
			          "             FROM " + InformationSchema.ThisUserSimpleGrantViewName +
			          "            WHERE \"object\" = 1 " +
			          "         GROUP BY \"param\" )" +
			          "   WHERE \"Tables.TABLE_SCHEMA\" = \"" + schema + "\" " +
			          "     AND CONCAT(\"Tables.TABLE_SCHEMA\", '.', \"Tables.TABLE_NAME\") = \"param\" ";
			return Show(context, sql, new SortColumn("Tables.TABLE_NAME"));
		}

		#region Prepared

		class Prepared : SqlStatement {
			public Prepared(SelectStatement select) {
				Select = select;
			}

			public SelectStatement Select { get; private set; }

			protected override void ExecuteStatement(ExecutionContext context) {
				var result = context.Request.ExecuteStatement(Select);
				context.SetResult(result);
			}
		}

		#endregion
	}
}
