// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
