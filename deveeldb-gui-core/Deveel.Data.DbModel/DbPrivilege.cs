using System;

namespace Deveel.Data.DbModel {
	public sealed class DbPrivilege : DbObject {
		public DbPrivilege(string schema, string tableName, string columnName, string name, string grantor, string grantee, bool grantable) 
			: base(schema, name, DbObjectType.Privilege) {
			this.tableName = tableName;
			this.columnName = columnName;
			this.grantor = grantor;
			this.grantee = grantee;
			this.grantable = grantable;
		}

		public DbPrivilege(string schema, string tableName, string name, string grantor, string grantee, bool grantable)
			: this(schema, tableName, null, name, grantor, grantee, grantable) {
		}

		private readonly string tableName;
		private readonly string columnName;
		private readonly string grantor;
		private readonly string grantee;
		private readonly bool grantable;

		public bool Grantable {
			get { return grantable; }
		}

		public string ColumnName {
			get { return columnName; }
		}

		public string Grantee {
			get { return grantee; }
		}

		public string Grantor {
			get { return grantor; }
		}

		public string TableName {
			get { return tableName; }
		}
	}
}