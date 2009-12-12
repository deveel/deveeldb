using System;

namespace Deveel.Data.Sql {
	internal class SqlTypeAttribute : IStatementTreeObject {
		private readonly string name;
		private readonly TType type;
		private readonly bool not_null;

		public SqlTypeAttribute(string name, TType type, bool notNull) {
			this.name = name;
			not_null = notNull;
			this.type = type;
		}

		public string Name {
			get { return name; }
		}

		public TType Type {
			get { return type; }
		}

		public bool NotNull {
			get { return not_null; }
		}

		#region Implementation of ICloneable

		public object Clone() {
			return new SqlTypeAttribute(name, type, not_null);
		}

		public void PrepareExpressions(IExpressionPreparer preparer) {
		}

		#endregion
	}
}