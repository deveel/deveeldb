using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbUniqueConstraint : DbConstraint {
		public DbUniqueConstraint(string schema, string table, string name) 
			: base(schema, table, name, DbConstraintType.Unique) {
			columns = new ArrayList();
		}

		private readonly ArrayList columns;

		public ArrayList Columns {
			get { return columns; }
		}
	}
}