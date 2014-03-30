using System;

namespace Deveel.Data.Types {
	public interface ITypeResolver {
		TType ResolveType(int typeCode);

		TType ResolveType(string typeString);
	}
}