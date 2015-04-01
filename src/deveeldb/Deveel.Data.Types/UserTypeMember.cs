using System;

namespace Deveel.Data.Types {
	public sealed class UserTypeMember {
		public UserTypeMember(string memberName, DataType memberType) {
			MemberName = memberName;
			MemberType = memberType;
		}

		public string MemberName { get; private set; }

		public DataType MemberType { get; private set; }
	}
}
