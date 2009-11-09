using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbPrimaryKey : DbConstraint {
		public DbPrimaryKey(string schema, string table, string name) 
			: base(schema, table, name, DbConstraintType.PrimaryKey) {
			columns = new ArrayList();
		}

		private readonly ArrayList columns;

		public IList Columns {
			get { return columns; }
		}
	}
}