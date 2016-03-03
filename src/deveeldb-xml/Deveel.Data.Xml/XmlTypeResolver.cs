using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlTypeResolver : ITypeResolver {
		public SqlType ResolveType(TypeResolveContext context) {
			if (String.Equals(context.TypeName, "XMLNODE", StringComparison.OrdinalIgnoreCase))
				return new XmlNodeType();
			return null;
		}
	}
}