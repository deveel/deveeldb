using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Linq {
	class ExpressionCompileContext {
		private Dictionary<string, string> typeAliases;
		private Dictionary<Type, string> typeAliaseseReverse;
		private List<Type> sources;
		private SqlExpression filter;
		private bool hasGroupBy;
		private List<SortColumn> sortColumns;

		public ExpressionCompileContext(CompiledModel model) {
			Model = model;
			typeAliases = new Dictionary<string, string>();
			typeAliaseseReverse = new Dictionary<Type, string>();

			sources = new List<Type>();

			Columns = new List<SelectColumn>();
			Parameters = new List<QueryParameter>();
		}

		public CompiledModel Model { get; private set; }

		public void AddAlias(Type type, string itemName, string aliasName) {
			if (!typeAliases.ContainsKey(itemName)) {
				typeAliases[aliasName] = itemName;
				typeAliaseseReverse[type] = aliasName;
			}
		}

		public ICollection<QueryParameter> Parameters { get; private set; }

		public IList<SelectColumn> Columns { get; private set; }

		public string FindTableName(Type type) {
			return Model.FindTableName(type);
		}

		public TypeMemberMapInfo GetMemberMap(Type type, string memberName) {
			var typeInfo = Model.GetTypeInfo(type);
			if (typeInfo == null)
				throw new InvalidOperationException(String.Format("Type '{0}' is not mapped.", type));

			var memberMapInfo = typeInfo.FindMemberMap(memberName);
			if (memberMapInfo == null)
				throw new InvalidOperationException(String.Format("Member '{0}' not found in type '{1}' or not mapped in the model", memberName, type));

			return memberMapInfo;
		}

		public void AddSource(Type type) {
			if (!sources.Contains(type))
				sources.Add(type);
		}

		public SelectStatement BuildQueryExpression() {
			List<SelectColumn> selected;
			if (Columns.Any()) {
				selected = Columns.ToList();
			} else {
				selected = sources.Select(x => SelectColumn.Glob(String.Format("{0}.*", typeAliaseseReverse[x]))).ToList();
			}

			var expression = new SqlQueryExpression(selected);

			// TODO : support joins

			foreach (var source in sources) {
				var tableName = Model.FindTableName(source);

				string alias;
				if (typeAliaseseReverse.TryGetValue(source, out alias)) {
					expression.FromClause.AddTable(alias, tableName);
				} else {
					expression.FromClause.AddTable(tableName);
				}
			}

			if (filter != null) {
				if (!hasGroupBy) {
					expression.WhereExpression = filter;
				} else {
					expression.HavingExpression = filter;
				}
			}

			var statement = new SelectStatement(expression);

			if (sortColumns != null) {
				statement.OrderBy = sortColumns.AsReadOnly();
			}

			return statement;
		}

		public void SetFilter(SqlExpression where) {
			filter = where;
		}

		public void SetHaving(SqlExpression having) {
			hasGroupBy = true;
			filter = having;
		}

		public void OrderBy(SqlExpression expression, bool ascending) {
			if (sortColumns == null)
				sortColumns = new List<SortColumn>();

			sortColumns.Add(new SortColumn(expression, ascending));
		}
	}
}
