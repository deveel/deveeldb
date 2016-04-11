using System;

namespace Deveel.Data.Mapping {
	public interface IMapResolver {
		TypeMapInfo ResolveTypeMap(Type type);
	}
}
