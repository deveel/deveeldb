// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.ObjectModel;
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The remote request of a SQL query against a system
	/// </summary>
	/// <remarks>
	/// <para>
	/// This object provides two main components for instructing to
	/// execute a query:
	/// <list type="buller">
	///   <listitem>The text of the command</listitem>
	///   <listitem>An optional list of parameters that will be applied 
	///   to the command</listitem>
	/// </list>
	/// </para>
	/// <para>
	/// While the text of the query is a required component of this request,
	/// the list of parameters is optional, until no parameter references are
	/// included in the text of the command.
	/// </para>
	/// <para>
	/// When creating a new <see cref="SqlQuery"/> it will be possible to
	/// specify the <see cref="SqlQueryParameterNaming"/> that will dictate
	/// the kind of parameter names tha will be accepted by this object, throwing
	/// an exception if the name of an added parameter does not respect it.
	/// </para>
	/// <para>
	/// If no specific <see cref="SqlQueryParameterNaming"/> is specified at
	/// construction of this object, the value of <see cref="ParameterNaming"/>
	/// will be set to <see cref="SqlQueryParameterNaming.Default"/> and the
	/// system will change it before execution.
	/// </para>
	/// <para>
	/// If the <see cref="ParameterNaming"/> is set to <see cref="SqlQueryParameterNaming.Default"/>,
	/// when the system will change to the configured default naming, all the parameter
	/// names contained into this query object will be re-validated.
	/// </para>
	/// </remarks>
	/// <seealso cref="SqlQueryParameterNaming"/>
	public sealed class SqlQuery {
		/// <summary>
		/// Constructs the query object with the given command
		/// text and the default parameter naming convention.
		/// </summary>
		/// <param name="text">The text of the command to be executed</param>
		/// <exception cref="ArgumentNullException">If the provided
		/// <paramref name="text"/> is <c>null</c> or empty.</exception>
		public SqlQuery(string text) 
			: this(text, SqlQueryParameterNaming.Default) {
		}

		/// <summary>
		/// Constructs the query object with the given command
		/// text and a given parameter naming convention.
		/// </summary>
		/// <param name="text">The text of the command to be executed</param>
		/// <param name="naming">The naming convention of the parameters in the query</param>
		/// <exception cref="ArgumentNullException">If the provided
		/// <paramref name="text"/> is <c>null</c> or empty.</exception>
		public SqlQuery(string text, SqlQueryParameterNaming naming) {
			if (String.IsNullOrEmpty(text))
				throw new ArgumentNullException(nameof(text));

			Text = text;
			ParameterNaming = naming;
			Parameters = new QueryParameterCollection(this);
		}

		/// <summary>
		/// Gets the text of the SQL command to be executed
		/// </summary>
		/// <remarks>
		/// The text can contain references to paramaters that will
		/// be extracted and validated, according to the naming
		/// convention specified.
		/// </remarks>
		/// <seealso cref="ParameterNaming"/>
		public string Text { get; }

		/// <summary>
		/// Gets a collection of parameters that will be applied to the
		/// command, according to the references provided in the text
		/// </summary>
		public IList<SqlQueryParameter> Parameters { get; }

		/// <summary>
		/// Gets the naming convention of the parameters in this query.
		/// </summary>
		public SqlQueryParameterNaming ParameterNaming { get; private set; }

		public ISqlExpressionPreparer ExpressionPreparer => new QueryPreparer(this);

		internal void ChangeNaming(SqlQueryParameterNaming naming) {
			if (ParameterNaming != SqlQueryParameterNaming.Default)
				throw new InvalidOperationException("Cannot change the parameter style if it was not set to default");
			if (naming == SqlQueryParameterNaming.Default)
				throw new ArgumentException("Cannot change the parameter style of a query to default");

			ParameterNaming = naming;
			((QueryParameterCollection)Parameters).ValidateAll();
		}

		#region QueryParameterCollection

		class QueryParameterCollection : Collection<SqlQueryParameter> {
			private SqlQuery SqlQuery { get; set; }

			public QueryParameterCollection(SqlQuery sqlQuery) {
				SqlQuery = sqlQuery;
			}

			private void ValidateParameter(SqlQueryParameter item, bool preventDuplicate) {
				if (item == null)
					throw new ArgumentNullException(nameof(item));

				if (SqlQuery.ParameterNaming == SqlQueryParameterNaming.Marker &&
					!String.Equals(item.Name, SqlQueryParameter.Marker, StringComparison.Ordinal))
					throw new ArgumentException(String.Format("The query accepts markers, but the parameter '{0}' is named.", item.Name));
				if (SqlQuery.ParameterNaming == SqlQueryParameterNaming.Named) {
					if (item.Name.Equals(SqlQueryParameter.Marker, StringComparison.Ordinal))
						throw new ArgumentException("The query accepts named parameters, but a marker was set.");

					if (preventDuplicate && Items.Any(x => String.Equals(x.Name, item.Name)))
						throw new ArgumentException(String.Format("A parameter named {0} was already inserted in the query.", item.Name));
				}
			}

			internal void ValidateAll() {
				foreach (var parameter in base.Items) {
					ValidateParameter(parameter, false);
				}
			}

			protected override void InsertItem(int index, SqlQueryParameter item) {
				ValidateParameter(item, true);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlQueryParameter item) {
				ValidateParameter(item, true);
				base.SetItem(index, item);
			}
		}

		#endregion

		#region QueryPreparer

		class QueryPreparer : ISqlExpressionPreparer {
			private readonly SqlQuery query;
			private int paramCount;

			public QueryPreparer(SqlQuery query) {
				this.query = query;
				paramCount = -1;
			}

			public bool CanPrepare(SqlExpression expression) {
				if (expression is SqlParameterExpression &&
				    query.ParameterNaming == SqlQueryParameterNaming.Marker) {
					return true;
				}
				if (expression is SqlVariableExpression &&
					query.ParameterNaming == SqlQueryParameterNaming.Named) {
					var varRef = (SqlVariableExpression) expression;
					return query.Parameters.Any(x => x.Name == varRef.VariableName);
				}

				return false;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				SqlQueryParameter param = null;
				if (expression is SqlParameterExpression) {
					var index = ++paramCount;
					param = query.Parameters[index];
				} else if (expression is SqlVariableExpression) {
					var varRef = (SqlVariableExpression) expression;

					param = query.Parameters.FirstOrDefault(x => x.Name == varRef.VariableName);
				}

				if (param == null)
					throw new SqlExpressionException("Could not determine a parameter in the query context");
				if (param.Direction != SqlParameterDirection.In)
					throw new SqlExpressionException($"Parameter {param.Name} is not INPUT");

				return SqlExpression.Constant(new SqlObject(param.SqlType, param.Value));
			}
		}

		#endregion
	}
}