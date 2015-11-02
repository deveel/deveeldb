using System;
using System.IO;
using System.Xml;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public sealed class XmlNodeType : SqlType {
		public XmlNodeType()
			: base("XMLNODE", SqlTypeCode.Type) {
		}

		static XmlNodeType() {
			XmlType = new XmlNodeType();
		}

		public static SqlType XmlType { get; private set; }

		public override object ConvertTo(ISqlObject obj, Type destType) {
			var xmlNode = obj as SqlXmlNode;
			if (xmlNode == null || xmlNode.IsNull)
				return null;

			if (destType == typeof (string))
				return xmlNode.ToString();
			if (destType == typeof (XmlNode))
				return xmlNode.ToXmlNode();
			if (destType == typeof (byte[]))
				return xmlNode.ToBytes();

			return base.ConvertTo(obj, destType);
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			return base.DeserializeObject(stream);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			base.SerializeObject(stream, obj);
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
					// TODO: more advanced casting...
					return DataObject.String(xmlNode.ToSqlString());
				case SqlTypeCode.Binary:
				case SqlTypeCode.LongVarBinary:
				case SqlTypeCode.VarBinary:
					// TODO: more advanced casting...
					return DataObject.Binary(xmlNode.ToSqlBinary());
				default:
					throw new InvalidCastException(String.Format("Cannot cast XML node to type '{0}'.", destType));
			}
		}
	}
}
