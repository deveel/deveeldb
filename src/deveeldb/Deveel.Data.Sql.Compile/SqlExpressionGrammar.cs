using System;

using Irony.Parsing;

namespace Deveel.Data.Sql.Compile {
	class SqlExpressionGrammar : SqlGrammarBase {
		public override string Dialect {
			get { return "EXPRESSION"; }
		}

		protected override NonTerminal MakeRoot() {
			return SqlExpression();
		}
	}
}
