using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql.Parser {
	public sealed class ValuesInsertNode : SqlNode {
		internal ValuesInsertNode() {
		}

		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public IEnumerable<InsertSetNode> Values { get; private set; } 
	}
}
