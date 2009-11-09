using System;

namespace Deveel.Data.DbModel {
	public sealed class DbDataType : DbObject {
		public DbDataType(string name, int size, int scale) 
			: base(null, name, DbObjectType.DataType) {
			this.size = size;
			this.scale = scale;
		}

		private readonly int size;
		private readonly int scale;

		public int Scale {
			get { return scale; }
		}

		public int Size {
			get { return size; }
		}
	}
}