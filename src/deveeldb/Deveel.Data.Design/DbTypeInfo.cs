using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbTypeInfo {
		internal DbTypeInfo(TypeBuildInfo buildInfo) {
			BuildInfo = buildInfo;
		}

		private TypeBuildInfo BuildInfo { get; set; }

		public string TableName {
			get { return BuildInfo.TableName; }
		}

		public Type Type {
			get { return BuildInfo.Type; }
		}

		public DbMemberInfo GetMember(string memberName) {
			var buildInfo = BuildInfo.GetMember(memberName);
			if (buildInfo == null)
				return null;

			return new DbMemberInfo(buildInfo);
		}

		internal IEnumerable<DbConstraintInfo> GetConstraints() {
			return BuildInfo.GetConstraints().Select(x => new DbConstraintInfo(x));
		}

		internal object CreateObject(Row row) {
			if (row == null)
				throw new ArgumentNullException("row");

			// TODO: Construct parametrized objects

			var obj = Activator.CreateInstance(Type, true);

			foreach (var memberInfo in BuildInfo.GetMembers().Select(x => new DbMemberInfo(x))) {
				memberInfo.ApplyFromRow(obj, row);
			}

			return obj;
		}
	}
}
