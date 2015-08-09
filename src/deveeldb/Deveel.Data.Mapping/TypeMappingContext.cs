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
