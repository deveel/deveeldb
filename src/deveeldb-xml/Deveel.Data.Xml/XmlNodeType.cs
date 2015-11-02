using System;

using Deveel.Data.Sql.Objects;
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

		public override DataObject CastTo(DataObject value, SqlType destType) {
			var xmlNode = value.Value as SqlXmlNode;
			if (xmlNode == null)
				return DataObject.Null(this);

			var destTypeCode = destType.TypeCode;
			switch (destTypeCode) {
				case SqlTypeCode.String:
				case SqlTypeCode.VarChar:
				case SqlTypeCode.LongVarChar:
					return ConvertToString(xmlNode, destType);
				case SqlTypeCode.Binary:
				case SqlTypeCode.LongVarBinary:
				case SqlTypeCode.VarBinary:
					return ConvertToBinary(xmlNode, destType);
				default:
					throw new InvalidCastException(String.Format("Cannot case "));
			}
		}

		private DataObject ConvertToBinary(SqlXmlNode xmlNode, SqlType destType) {
			throw new NotImplementedException();
		}

		private DataObject ConvertToString(SqlXmlNode xmlNode, SqlType destType) {
			throw new NotImplementedException();
		}
	}
}
