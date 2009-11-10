using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbColumn : DbObject, IDbGrantableObject {
		public DbColumn(string schema, string tableName, string name, DbDataType dataType, int size) 
			: base(schema, name, DbObjectType.Column) {
			this.tableName = tableName;
			this.dataType = dataType;
			this.size = size;
			privileges = new ArrayList();
		}

		public DbColumn(string schema, string tableName, string name, DbDataType dataType) 
			: this(schema, tableName, name, dataType, -1) {
		}

		private readonly string tableName;
		private readonly DbDataType dataType;
		private int size;
		private int scale;
		private string defaultExpression;
		private readonly ArrayList privileges;

		public int Size {
			get { return size; }
			set { size = value; }
		}

		public DbDataType DataType {
			get { return dataType; }
		}

		public int Scale {
			get { return scale; }
			set { scale = value; }
		}

		public string TableName {
			get { return tableName; }
		}

		public string Default {
			get { return defaultExpression; }
			set { defaultExpression = value; }
		}

		public IList Privileges {
			get { return (IList) privileges.Clone(); }
		}

		public override string FullName {
			get { return base.FullName + "." + Name; }
		}

		public void AddPrivilege(DbPrivilege privilege) {
			if (privilege == null)
				throw new ArgumentNullException("privilege");

			privileges.Add(privilege);
		}

		public DbPrivilege AddPrivilege(string privilege, string grantor, string grantee, bool grantable) {
			if (privilege == null || privilege.Length == 0)
				throw new ArgumentNullException("privilege");

			DbPrivilege dbPrivilege = new DbPrivilege(Schema, tableName, Name, privilege, grantor, grantee, grantable);
			privileges.Add(dbPrivilege);
			return dbPrivilege;
		}
	}
}