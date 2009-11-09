using System;

namespace Deveel.Data.DbModel {
	public abstract class DbConstraint : DbObject {
		protected DbConstraint(string schema, string table, string name, DbConstraintType type) 
			: base(schema, name, DbObjectType.Constraint) {
			this.table = table;
			this.type = type;
		}

		private readonly string table;
		private readonly DbConstraintType type;

		public string Table {
			get { return table; }
		}

		public DbConstraintType ConstraintType {
			get { return type; }
		}
	}
}