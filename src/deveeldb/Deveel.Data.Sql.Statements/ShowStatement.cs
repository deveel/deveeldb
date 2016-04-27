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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

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
				tableName = context.Access().ResolveTableName(TableName);
			}

			if (Target == ShowTarget.Schema)
				return ShowSchema();
			if (Target == ShowTarget.SchemaTables)
				return ShowSchemaTables(context.Query.CurrentSchema());
			if (Target == ShowTarget.Table)
				return ShowTable(tableName);
			if (Target == ShowTarget.Product)
				return ShowProduct();

			throw new StatementException(String.Format("The SHOW target {0} is not supported.", Target));
		}

		private static SqlStatement Show(string sql, params QueryParameter[] parameters) {
			var query = new SqlQuery(sql);
			if (parameters != null && parameters.Length > 0) {
				foreach (var parameter in parameters) {
					query.Parameters.Add(parameter);
				}
			}

			return new Prepared(query);
		}

		private SqlStatement ShowSchema() {
			var sql = "SELECT \"name\" AS \"schema_name\", " +
			          "       \"type\", " +
			          "       \"other\" AS \"notes\" " +
			          "    FROM " + InformationSchema.ThisUserSchemaInfoViewName + " " +
					  "ORDER BY \"schema_name\"";

			return Show(sql);
		}

		private SqlStatement ShowProduct() {
			const string sql = "SELECT \"name\", \"version\" FROM " +
			                   "  ( SELECT \"value\" AS \"name\" FROM SYSTEM.product_info " +
			                   "     WHERE \"var\" = 'name' ), " +
			                   "  ( SELECT \"value\" AS \"version\" FROM SYSTEM.product_info " +
			                   "     WHERE \"var\" = 'version' ) ";

			return Show(sql);
		}

		private SqlStatement ShowSchemaTables(string schema) {
			var sql = "  SELECT \"Tables.TABLE_NAME\" AS \"table_name\", " +
			          "         I_PRIVILEGE_STRING(\"agg_priv_bit\") AS \"user_privs\", " +
			          "         \"Tables.TABLE_TYPE\" as \"table_type\" " +
			          "    FROM " + InformationSchema.Tables + ", " +
			          "         ( SELECT AGGOR(\"priv_bit\") agg_priv_bit, " +
			          "                  \"object\", \"name\" " +
			          "             FROM " + InformationSchema.ThisUserSimpleGrantViewName +
			          "            WHERE \"object\" = " + ((int)DbObjectType.Table) +
			          "         GROUP BY \"name\" )" +
			          "   WHERE \"Tables.TABLE_SCHEMA\" = ? " +
			          "     AND CONCAT(\"Tables.TABLE_SCHEMA\", '.', \"Tables.TABLE_NAME\") = \"name\" " +
					  "ORDER BY Tables.TABLE_NAME";

			var param = new QueryParameter(PrimitiveTypes.String(), new SqlString(schema));
			return Show(sql, param);
		}

		private SqlStatement ShowTable(ObjectName tableName) {
			var sql = "  SELECT \"column\" AS \"name\", " +
			          "         i_sql_type(\"type_desc\", \"size\", \"scale\") AS \"type\", " +
			          "         \"not_null\", " +
			          "         \"index_str\" AS \"index\", " +
			          "         \"default\" " +
			          "    FROM " + InformationSchema.ThisUserTableColumnsViewName + " " +
			          "   WHERE \"schema\" = ? " +
			          "     AND \"table\" = ? " +
			          "ORDER BY \"seq_no\" ";

			var parameters = new[] {
				new QueryParameter(PrimitiveTypes.String(), new SqlString(tableName.ParentName)),
				new QueryParameter(PrimitiveTypes.String(), new SqlString(tableName.Name)) 
			};

			return Show(sql, parameters);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(SqlQuery query) {
				Query = query;
			}

			private Prepared(SerializationInfo info, StreamingContext context)
				: base(info, context) {
				Query = (SqlQuery) info.GetValue("Query", typeof(SqlQuery));
			}

			public SqlQuery Query { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("Query", Query);
				base.GetData(info);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				var results = context.Query.ExecuteQuery(Query);

				if (results.Length != 1)
					throw new StatementException("Too many queries were executed.");

				var result = results[0];

				if (result.Type == StatementResultType.Exception)
					throw result.Error;

				if (result.Type != StatementResultType.CursorRef)
					throw new StatementException("Invalid result for query");

				context.SetCursor(result.Cursor);
			}
		}

		#endregion
	}
}
