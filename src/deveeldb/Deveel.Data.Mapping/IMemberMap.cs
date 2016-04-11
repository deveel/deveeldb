using System;

namespace Deveel.Data.Mapping {
	interface IMemberMap {
		string MemberName { get; }

		MemberMapInfo GetMapInfo(TypeMapInfo typeInfo);
	}
}
