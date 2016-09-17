using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.Mapping;

using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlSelectGeneratorExpressionVisitor : RelinqExpressionVisitor {
		private IDictionary<string, string> sources;
		private string lastSource;
		private IDictionary<string, List<string>> items;

		private SqlSelectGeneratorExpressionVisitor(IDictionary<string, string> sources) {
			items = new Dictionary<string, List<string>>();
			this.sources = sources;
		}

		public static string GetSqlExpression(Expression selectExpression, IDictionary<string, string> sources) {
			var visitor = new SqlSelectGeneratorExpressionVisitor(sources);
			visitor.Visit(selectExpression);
			return visitor.GetSqlExpression();
		}

		private string GetSqlExpression() {
			var list = new List<string>();

			foreach (var itemGroup in items) {
				string tableName;
				if (!sources.TryGetValue(itemGroup.Key, out tableName))
					throw new InvalidOperationException();

				var members = itemGroup.Value;
				if (members == null || members.Count == 0) {
					list.Add(String.Format("{0}.*", tableName));
				} else {
					list.AddRange( members.Select(x => String.Format("{0}.{1}", tableName, x)));
				}
			}

			return String.Join(", ", list.ToArray());
		}

		protected override Expression VisitMember(MemberExpression expression) {
			var type = expression.Member.ReflectedType;
			var typeInfo = Mapper.GetMapInfo(type);
			var memberName = expression.Member.Name;

			var memberInfo = typeInfo.Members.FirstOrDefault(x => x.Member.Name == memberName);
			if (memberInfo == null)
				throw new NotSupportedException();

			List<string> members;
			if (!items.TryGetValue(lastSource, out members))
				throw new InvalidOperationException();

			members.Add(memberInfo.ColumnName);

			return expression;
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
			lastSource = expression.ReferencedQuerySource.ItemName;
			items[lastSource] = new List<string>();
			return expression;
		}
	}
}
