using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class DbColumnModel : IDbModel {
		internal DbColumnModel(MemberInfo member, string columnName, bool isKey) {
			Member = member;
			ColumnName = columnName;
			IsKey = isKey;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public bool IsKey { get; private set; }
	}
}
