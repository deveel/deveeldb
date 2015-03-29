using System;

namespace Deveel.Data.Types {
	public interface IDataTypeResolver {
		DataType ResolveType(ObjectName name);
	}
}
