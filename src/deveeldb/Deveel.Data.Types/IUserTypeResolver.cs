using System;

namespace Deveel.Data.Types {
	public interface IUserTypeResolver {
		UserType ResolveType(ObjectName typeName);
	}
}
