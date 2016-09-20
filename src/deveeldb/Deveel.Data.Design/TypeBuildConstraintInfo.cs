using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class TypeBuildConstraintInfo {
		internal TypeBuildConstraintInfo(TypeBuildInfo typeInfo, string name, ConstraintType constraintType) {
			TypeInfo = typeInfo;
			ConstraintName = name;
			ConstraintType = constraintType;
			Members = new List<TypeBuildMemberInfo>();
		}

		public ConstraintType ConstraintType { get; private set; }

		public string ConstraintName { get; private set; }

		public bool IsNamed {
			get { return !String.IsNullOrEmpty(ConstraintName); }
		}

		public TypeBuildInfo TypeInfo { get; private set; }

		public ICollection<TypeBuildMemberInfo> Members { get; private set; }
	}
}
