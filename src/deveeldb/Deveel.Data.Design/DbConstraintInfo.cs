using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbConstraintInfo {
		private IEqualityComparer<DbMemberInfo> uniqueMemberComparer;

		internal DbConstraintInfo(ConstraintModelConfiguration configuration) {
			Configuration = configuration;
			uniqueMemberComparer = new UniqueMemberComparer();
		}

		private ConstraintModelConfiguration Configuration { get; set; }

		public DbTypeInfo TypeInfo {
			get { return new DbTypeInfo(Configuration.TypeModel); }
		}

		public ConstraintType ConstraintType {
			get { return Configuration.ConstraintType; }
		}

		public IEnumerable<DbMemberInfo> Members {
			get { return Configuration.Members.Select(x => new DbMemberInfo(x)).Distinct(uniqueMemberComparer); }
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
