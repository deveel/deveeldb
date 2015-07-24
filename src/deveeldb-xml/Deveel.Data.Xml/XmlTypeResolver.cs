using System;

using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlTypeResolver : ITypeResolver {
		public DataType ResolveType(string typeName, params DataTypeMeta[] metadata) {
			if (String.Equals(typeName, "XMLNODE", StringComparison.OrdinalIgnoreCase))
				return new XmlNodeType();
			return null;
		}
	}
}