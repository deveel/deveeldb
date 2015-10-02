// 
//  Copyright 2010-2015 Deveel
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
using System.Linq;

namespace Deveel.Data.Mapping {
	public class TypeMappingContext : ITypeMappingContext {
		private bool built;
		private ICollection<TypeMapping> mappings;

		public TypeMappingContext() {
			TableNamingConvention = RuledNamingConvention.SqlNaming;
			ColumNamingConvention = RuledNamingConvention.SqlNaming;
		}

		public INamingConvention TableNamingConvention { get; set; }

		public INamingConvention ColumNamingConvention { get; set; }

		protected virtual void OnBuild(ICollection<TypeMapping> mappings) {
		}

		public TypeMapping GetMapping(Type type) {
			if (!built) {
				mappings = new List<TypeMapping>();
				OnBuild(mappings);
				built = true;
			}

			if (mappings == null ||
			    mappings.Count == 0)
				return null;

			return mappings.FirstOrDefault(x => x.Type == type);
		}
	}
}
