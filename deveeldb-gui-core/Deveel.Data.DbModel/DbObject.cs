using System;

namespace Deveel.Data.DbModel {
	public class DbObject : IDbObject {
		public DbObject(string schema, string name, DbObjectType type) {
			this.name = name;
			this.schema = schema;
			this.type = type;
		}

		private readonly string name;
		private readonly string schema;
		private readonly DbObjectType type;

		public string Schema {
			get { return schema; }
		}

		public string Name {
			get { return name; }
		}

		public DbObjectType ObjectType {
			get { return type; }
		}

		public virtual string FullName {
			get {
				string fullName = name;
				if (schema != null && schema.Length > 0)
					fullName = schema + "." + fullName;
				return fullName;
			}
		}

		public override string ToString() {
			return FullName;
		}
	}
}