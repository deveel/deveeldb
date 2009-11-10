using System;

namespace Deveel.Data.DbModel {
	public sealed class DbDataType : DbObject {
		public DbDataType(string name, SqlType sqlType) 
			: base(null, name, DbObjectType.DataType) {
			this.sqlType = sqlType;
		}

		private readonly SqlType sqlType;
		private int precision;
		private string literalPrefix;
		private string literalSuffix;
		private bool searchable;

		public SqlType SqlType {
			get { return sqlType; }
		}

		public int Precision {
			get { return precision; }
			set { precision = value; }
		}

		public string LiteralPrefix {
			get { return literalPrefix; }
			set { literalPrefix = value; }
		}

		public string LiteralSuffix {
			get { return literalSuffix; }
			set { literalSuffix = value; }
		}

		public bool Searchable {
			get { return searchable; }
			set { searchable = value; }
		}

		public string GetStringValue(object value) {
			if (value == null || value == DBNull.Value)
				return "NULL";

			return String.Concat(LiteralPrefix, value, LiteralSuffix);
		}
	}
}