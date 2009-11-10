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

		public DbForeignKey(string schema, string table, string name) 
			: this(schema, table, name, null) {
		}

		private string referenceSchema;
		private string referenceTable;
		private readonly ArrayList columns;
		private readonly ArrayList refColumns;
		private string onDelete;
		private string onUpdate;

		public IList ReferenceColumns {
			get { return (IList) refColumns.Clone(); }
		}

		public IList Columns {
			get { return (IList) columns.Clone(); }
		}

		public string ReferenceTable {
			get { return referenceTable; }
			set { referenceTable = value; }
		}

		public string OnDelete {
			get { return onDelete; }
			set { onDelete = value; }
		}

		public string OnUpdate {
			get { return onUpdate; }
			set { onUpdate = value; }
		}

		public string ReferenceSchema {
			get { return referenceSchema; }
			set { referenceSchema = value; }
		}

		public void AddColumn(DbColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			if (column.Schema != Schema ||
				column.TableName != Table)
				throw new ArgumentException();

			columns.Add(column);
		}

		public void AddReferenceColumn(DbColumn column) {
			throw new NotImplementedException();
		}
	}
}