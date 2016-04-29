using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public class DeclaredAliasGatherer : QueryExpressionVisitor {
		private readonly HashSet<Alias> aliases;

		private DeclaredAliasGatherer() {
			this.aliases = new HashSet<Alias>();
		}

		public static HashSet<Alias> Gather(Expression source) {
			var gatherer = new DeclaredAliasGatherer();
			gatherer.Visit(source);
			return gatherer.aliases;
		}

		protected override Expression VisitSelect(SelectExpression select) {
			this.aliases.Add(select.Alias);
			return select;
		}

		protected override Expression VisitTable(TableExpression table) {
			this.aliases.Add(table.Alias);
			return table;
		}
	}
}
