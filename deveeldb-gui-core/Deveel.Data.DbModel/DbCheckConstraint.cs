using System;

namespace Deveel.Data.DbModel {
	public sealed class DbCheckConstraint : DbConstraint {
		public DbCheckConstraint(string schema, string table) 
			: base(schema, table, null, DbConstraintType.Check) {
		}

		private string expression;

		public string Expression {
			get { return expression; }
			set { expression = value; }
		}
	}
}