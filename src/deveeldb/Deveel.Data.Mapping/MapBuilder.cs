using System;
using System.Collections.Generic;

namespace Deveel.Data.Mapping {
	public sealed class MapBuilder {
		private readonly Dictionary<string, ITypeMap> types;

		public MapBuilder() {
			types = new Dictionary<string, ITypeMap>();
		}

		public void AddMap<TType>(TypeMap<TType> typeMap) where TType : class {
			var typeName = typeof(TType).Name;
			types[typeName] = typeMap;
		}
		 
		public TypeMap<TType> Type<TType>() where TType : class {
			var typeName = typeof (TType).Name;
			ITypeMap typeMap;
			if (!types.TryGetValue(typeName, out typeMap)) {
				typeMap = new TypeMap<TType>();
				types[typeName] = typeMap;
			}

			return (TypeMap<TType>) typeMap;
		}

		internal CompiledMap Compile() {
			var compiled = new CompiledMap();

			foreach (var typeMap in types) {
				var info = typeMap.Value.GetMapInfo();
				compiled.AddTypeInfo(info);
			}

			return compiled;
		}
	}
}
