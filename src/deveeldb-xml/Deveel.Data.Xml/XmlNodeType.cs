using System;

using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlNodeType : DataType {
		public XmlNodeType()
			: base("XMLNODE", SqlTypeCode.Type) {
		}

		public override bool CanCastTo(DataType type) {
			return type is StringType ||
			       type is BinaryType;
		}
	}
}
