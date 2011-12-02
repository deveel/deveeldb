using System;
using System.Collections;

namespace Deveel.Data.Shell {
	public sealed class ForeignKey {
		private readonly string name;
		private readonly string refSchema;
		private readonly string refTable;
		private readonly ArrayList pkColumns;
		private readonly ArrayList fkColumns;
		private readonly int deferrability;
		private readonly string updateRule;
		private readonly string deleteRule;

		internal ForeignKey(string name, string refSchema, string refTable, string updateRule, string deleteRule, int deferrability) {
			this.name = name;
			this.refSchema = refSchema;
			this.refTable = refTable;
			this.updateRule = updateRule;
			this.deleteRule = deleteRule;
			this.deferrability = deferrability;
			pkColumns = new ArrayList();
			fkColumns = new ArrayList();
		}

		public string Name {
			get { return name; }
		}

		public string ReferencedSchema {
			get { return refSchema; }
		}

		public string ReferencedTable {
			get { return refTable; }
		}

		public string DeleteRule {
			get { return deleteRule; }
		}

		public string UpdateRule {
			get { return updateRule; }
		}

		public int Deferrability {
			get { return deferrability; }
		}

		public ICollection PrimaryColumns {
			get { return pkColumns; }
		}

		public ICollection ReferencedColumns {
			get { return fkColumns; }
		}

		internal void AddPkColumn(string columnName) {
			pkColumns.Add(columnName);
		}

		internal void AddFkColumn(string columnName) {
			fkColumns.Add(columnName);
		}
	}
}