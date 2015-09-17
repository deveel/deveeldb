using System;

using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlNodeType : SqlType {
		public XmlNodeType()
			: base("XMLNODE", SqlTypeCode.Type) {
		}

		public override bool CanCastTo(SqlType destType) {
			return destType is StringType ||
			       destType is BinaryType;
		}
	}
}
