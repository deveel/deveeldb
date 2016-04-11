using System;
using System.Collections.Generic;
using System.Data.Odbc;

namespace Deveel.Data.Mapping {
	class CompiledMap : IMapResolver, IDisposable {
		private Dictionary<Type, TypeMapInfo> typeMaps;

		public CompiledMap() {
			typeMaps = new Dictionary<Type, TypeMapInfo>();
		}

		~CompiledMap() {
			Dispose(false);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (typeMaps != null)
					typeMaps.Clear();
			}

			typeMaps = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public TypeMapInfo ResolveTypeMap(Type type) {
			TypeMapInfo typeInfo;
			if (!typeMaps.TryGetValue(type, out typeInfo))
				return null;

			return typeInfo;
		}

		public void AddTypeInfo(TypeMapInfo typeMapInfo) {
			typeMaps[typeMapInfo.Type] = typeMapInfo;
		}
	}
}
