using System;
using System.Reflection;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMapping {
		public MemberMapping(MemberInfo member, string columnName, SqlType columnType) {
			if (member == null)
				throw new ArgumentNullException("member");

			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public string ColumnName { get; private set; }

		public MemberInfo Member { get; private set; }

		public string MemberName {
			get { return Member.Name; }
		}

		public Type MemberType {
			get {
				if (Member is PropertyInfo)
					return ((PropertyInfo) Member).PropertyType;
				if (Member is FieldInfo)
					return ((FieldInfo) Member).FieldType;

				throw new InvalidOperationException("Invalid member");
			}
		}

		public SqlType ColumnType { get; private set; }

		public bool IsNotNull { get; set; }

		public static MemberMapping CreateFrom(MemberInfo member, ITypeMappingContext mappingContext) {
			throw new NotImplementedException();
		}
	}
}
