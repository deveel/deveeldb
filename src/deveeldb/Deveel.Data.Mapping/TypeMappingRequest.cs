using System;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class TypeMappingRequest {
		public TypeMappingRequest(Type type, ITypeMappingContext mappingContext) 
			: this(type, mappingContext, null) {
		}

		public TypeMappingRequest(Type type, ITypeMappingContext mappingContext, ITypeResolver typeResolver) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (mappingContext == null)
				throw new ArgumentNullException("mappingContext");

			Type = type;
			MappingContext = mappingContext;
			TypeResolver = typeResolver;
		}

		public Type Type { get; private set; }

		public ITypeMappingContext MappingContext { get; private set; }

		public ITypeResolver TypeResolver { get; private set; }
	}
}
