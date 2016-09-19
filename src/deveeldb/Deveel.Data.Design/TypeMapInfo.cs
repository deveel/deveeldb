using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class TypeMapInfo {
		internal TypeMapInfo(Type type, string tableName, IEnumerable<TypeMemberMapInfo> members, IEnumerable<TypeConstraintMapInfo> constraints) {
			Type = type;
			TableName = tableName;
			Members = members;
			Constraints = constraints;
		}

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		public IEnumerable<TypeMemberMapInfo> Members { get; private set; }

		public IEnumerable<TypeConstraintMapInfo> Constraints { get; private set; }

		internal TypeMemberMapInfo FindMemberMap(string memberName) {
			if (Members == null)
				return null;

			var members = Members.ToDictionary(x => x.Member.Name, y => y);
			TypeMemberMapInfo memberInfo;

			if (!members.TryGetValue(memberName, out memberInfo))
				return null;

			return memberInfo;
		}

		internal object Construct(Row row) {
			throw new NotImplementedException();
		}
	}
}
