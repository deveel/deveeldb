using System;
using System.Reflection;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public sealed class TypeBuildMemberInfo {
		internal TypeBuildMemberInfo(TypeBuildInfo typeInfo, MemberInfo member) {
			TypeInfo = typeInfo;
			Member = member;
		}

		public MemberInfo Member { get; private set; }

		public Type MemberType {
			get {
				if (Member is PropertyInfo)
					return ((PropertyInfo) Member).PropertyType;
				if (Member is FieldInfo)
					return ((FieldInfo) Member).FieldType;

				throw new InvalidOperationException();
			}
		}

		public TypeBuildInfo TypeInfo { get; private set; }

		public string ColumnName { get; set; }

		public SqlType ColumnType { get; set; }

		public bool NotNull { get; set; }
	}
}
