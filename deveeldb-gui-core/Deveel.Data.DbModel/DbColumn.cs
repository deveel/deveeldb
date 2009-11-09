using System;

namespace Deveel.Data.DbModel {
	public sealed class DbColumn : DbObject {
		public DbColumn(string schema, string name, DbDataType dataType) 
			: base(schema, name, DbObjectType.Column) {
			this.dataType = dataType;
		}

		private readonly DbDataType dataType;
	}
}