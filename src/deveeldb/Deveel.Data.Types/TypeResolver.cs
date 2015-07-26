using System;

namespace Deveel.Data.Types {
	static class TypeResolver {
		public static DataType Resolve(SqlTypeCode typeCode, string typeName, DataTypeMeta[] metadata, ITypeResolver resolver) {
			if (PrimitiveTypes.IsPrimitive(typeCode))
				return PrimitiveTypes.Resolve(typeCode, typeName, metadata);

			if (resolver == null)
				throw new NotSupportedException(String.Format("Cannot resolve type '{0}' without context.", typeName));

			var resolveCcontext = new TypeResolveContext(typeCode, typeName, metadata);
			return resolver.ResolveType(resolveCcontext);
		}
	}
}
