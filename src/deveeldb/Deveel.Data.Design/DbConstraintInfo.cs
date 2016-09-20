using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbConstraintInfo {
		private IEqualityComparer<DbMemberInfo> uniqueMemberComparer;

		internal DbConstraintInfo(TypeBuildConstraintInfo constraintInfo) {
			ConstraintInfo = constraintInfo;
			uniqueMemberComparer = new UniqueMemberComparer();
		}

		private TypeBuildConstraintInfo ConstraintInfo { get; set; }

		public DbTypeInfo TypeInfo {
			get { return new DbTypeInfo(ConstraintInfo.TypeInfo); }
		}

		public ConstraintType ConstraintType {
			get { return ConstraintInfo.ConstraintType; }
		}

		public IEnumerable<DbMemberInfo> Members {
			get { return ConstraintInfo.Members.Select(x => new DbMemberInfo(x)).Distinct(uniqueMemberComparer); }
		}

		#region UniqueMemberComparer

		class UniqueMemberComparer : IEqualityComparer<DbMemberInfo> {
			public bool Equals(DbMemberInfo x, DbMemberInfo y) {
				return x.Member.Name == y.Member.Name;
			}

			public int GetHashCode(DbMemberInfo obj) {
				return obj.Member.Name.GetHashCode();
			}
		}

		#endregion
	}
}
