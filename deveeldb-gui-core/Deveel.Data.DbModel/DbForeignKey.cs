using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbForeignKey : DbConstraint {
		public DbForeignKey(string schema, string table, string name, string referenceTable) 
			: base(schema, table, name, DbConstraintType.ForeignKey) {
			this.referenceTable = referenceTable;
			columns = new ArrayList();
			refColumns = new ArrayList();
		}

		private readonly string referenceTable;
		private readonly ArrayList columns;
		private readonly ArrayList refColumns;
		private string onDelete;
		private string onUpdate;

		public ArrayList ReferenceColumns {
			get { return refColumns; }
		}

		public ArrayList Columns {
			get { return columns; }
		}

		public string ReferenceTable1 {
			get { return referenceTable; }
		}

		public string OnDelete {
			get { return onDelete; }
			set { onDelete = value; }
		}

		public string OnUpdate {
			get { return onUpdate; }
			set { onUpdate = value; }
		}
	}
}