using System;

using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlTypeResolver : ITypeResolver {
		public DataType ResolveType(TypeResolveContext context) {
			if (String.Equals(context.TypeName, "XMLNODE", StringComparison.OrdinalIgnoreCase))
				return new XmlNodeType();
			return null;
		}
	}
}