using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbTable : DbObject {
		public DbTable(string schema, string name) 
			: base(schema, name, DbObjectType.Table) {
			objects = new ArrayList();
		}

		private readonly ArrayList objects;

		public IList Objects {
			get { return (IList) objects.Clone(); }
		}

		public void AddColumn(DbColumn column) {
			
		}

		public void AddColumn(string name, DbDataType type) {
			
		}

		public void AddConstraint(DbConstraint constraint) {
			
		}
	}
}