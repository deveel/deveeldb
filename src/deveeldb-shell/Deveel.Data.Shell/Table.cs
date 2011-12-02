using System;
using System.Collections;
using System.Text;

namespace Deveel.Data.Shell {
	public sealed class Table : IComparable {
		private readonly string schema;
		private readonly string name;
		private readonly string type;
		private readonly Hashtable columns;
		private readonly ArrayList privileges;
		private readonly Hashtable foreignKeys;
		private PrimaryKey primaryKey;

		internal Table(string schema, string name, string type) {
			this.name = name;
			this.schema = schema;
			this.type = type;
			columns = new Hashtable();
			privileges = new ArrayList();
			foreignKeys = new Hashtable();
		}

		public String Name {
			get { return name; }
		}

		public string Schema {
			get { return schema; }
		}

		public string Type {
			get { return type; }
		}

		public PrimaryKey PrimaryKey {
			get { return primaryKey; }
		}

		internal void AddColumn(Column column) {
			columns.Add(column.Name, column);
		}

		internal void AddPrivilege(Privilege privilege) {
			privileges.Add(privilege);
		}

		internal void AddForeignKey(ForeignKey fkey) {
			foreignKeys.Add(fkey.Name, fkey);
		}

		internal void SetPrimaryKey(PrimaryKey pkey) {
			primaryKey = pkey;
		}

		public bool HasColumn(string column) {
			return columns.ContainsKey(column);
		}

		public Column getColumnByName(String name, bool ignoreCase) {
			Column result = null;
			if (columns != null) {
				result = (Column)columns[name];
				if (result == null && ignoreCase) {
					IEnumerator iter = columns.Keys.GetEnumerator();
					while (iter.MoveNext()) {
						String colName = (String)iter.Current;
						if (String.Compare(colName, name, true) == 0) {
							result = (Column)columns[colName];
							break;
						}
					}
				}
			}
			return result;
		}

		public ForeignKey GetForeignKey(string fkeyName) {
			return foreignKeys[fkeyName] as ForeignKey;
		}

		public bool HasForeignKeys {
			get { return foreignKeys.Count > 0; }
		}

		public ICollection ForeignKeys {
			get { return foreignKeys.Values; }
		}

		public override String ToString() {
			StringBuilder sb = new StringBuilder();
			if (schema != null)
				sb.Append(schema).Append(".");
			sb.Append(name);
			return sb.ToString();
		}

		public override int GetHashCode() {
			return name.GetHashCode() + schema != null ? schema.GetHashCode() : 0;
		}

		public override bool Equals(Object obj) {
			Table table = obj as Table;
			if (table == null)
				return false;

			if (schema != null && table.schema == null)
				return false;

			if (schema != table.schema)
				return false;

			return name == table.name;
		}

		public int CompareTo(Object other) {
			int result = 0;
			if (other is Table) {
				Table o = (Table)other;
				result = name.CompareTo(o.Name);
			}
			return result;
		}

		public PrimaryKey GetPrimaryKey(string keyName) {
			return null;
		}
	}
}