using System;

using Irony.Parsing;

namespace Deveel.Data.Sql.Compile {
	class SqlDataTypeGrammar : SqlGrammarBase {
		public override string Dialect {
			get { return "DATA-TYPE"; }
		}

		protected override NonTerminal MakeRoot() {
			return DataType();
		}
	}
}
