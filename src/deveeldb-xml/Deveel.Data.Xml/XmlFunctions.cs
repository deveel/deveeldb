using System;
using System.IO;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public static class XmlFunctions {
		public static IRoutineResolver Resolver {
			get { return new XmlFunctionProvider(); }
		}

		private static string GetXPath(Field xpath) {
			if (!(xpath.Type is StringType))
				throw new ArgumentException();

			return xpath.AsVarChar().Value.ToString();
		}

		private static SqlXmlNode GetXmlNode(Field node) {
			if (!(node.Type is XmlNodeType))
				throw new ArgumentException();

			return node.Value as SqlXmlNode;
		}

		public static Field XmlType(Field obj) {
			if (obj.IsNull)
				return new Field(XmlNodeType.XmlType, SqlNull.Value);

			var value = obj.Value;
			SqlXmlNode xmlNode;

			if (value is ISqlBinary) {
				xmlNode = XmlType((ISqlBinary) value);
			} else if (value is ISqlString) {
				xmlNode = XmlType((ISqlString) value);
			} else {
				throw new NotSupportedException();
			}

			return new Field(XmlNodeType.XmlType, xmlNode);
		}

		public static SqlXmlNode XmlType(ISqlBinary binary) {
			var len = binary.Length;
			var content = new byte[len];
			var offset = 0;

			const int bufferSize = 1024 * 10;

			using (var stream = binary.GetInput()) {
				using (var reader = new BinaryReader(stream)) {
					while (true) {
						var buffer = new byte[bufferSize];
						var readCount = reader.Read(buffer, 0, bufferSize);

						Array.Copy(buffer, 0, content, offset, readCount);

						if (readCount == 0)
							break;

						offset += readCount;
					}
				}
			}

			return new SqlXmlNode(content);
		}

		public static SqlXmlNode XmlType(ISqlString s) {
			var len = s.Length;
			var content = new char[len];
			var offset = 0;

			const int bufferSize = 1024*10;

			using (var reader = s.GetInput(s.Encoding)) {
				while (true) {
					var buffer = new char[bufferSize];
					var readCount = reader.Read(buffer, 0, bufferSize);

					if (readCount == 0)
						break;

					Array.Copy(buffer, 0, content, offset, readCount);

					offset += readCount;
				}
			}

			var bytes = s.Encoding.GetBytes(content);
			if (!s.Encoding.Equals(Encoding.UTF8))
				bytes = Encoding.Convert(s.Encoding, Encoding.UTF8, bytes);

			return new SqlXmlNode(bytes);
		}

		public static Field AppendChild(Field obj, Field xpath, Field value) {
			var result = AppendChild(GetXmlNode(obj), GetXPath(xpath), GetXmlNode(value));
			return new Field(XmlNodeType.XmlType, result);
		}


		public static SqlXmlNode AppendChild(SqlXmlNode node, string xpath, SqlXmlNode value) {
			return node.AppendChild(xpath, value);
		}

		public static Field Extract(Field obj, Field xpath) {
			var result = Extract(GetXmlNode(obj), GetXPath(xpath));
			return new Field(XmlNodeType.XmlType, result);
		}

		public static SqlXmlNode Extract(SqlXmlNode node, string xpath) {
			return node.Extract(xpath);
		}

		public static Field ExtractValue(Field obj, Field xpath) {
			var result = ExtractValue(GetXmlNode(obj), GetXPath(xpath));

			SqlType resultType = PrimitiveTypes.String();
			if (result is ISqlBinary)
				resultType = PrimitiveTypes.Binary();
			else if (result is SqlNumber)
				resultType = PrimitiveTypes.Numeric();

			// TODO: Support more types

			return new Field(resultType, result);
		}

		public static ISqlObject ExtractValue(SqlXmlNode node, string xpath) {
			return node.ExtractValue(xpath);
		}

		public static Field Delete(Field obj, Field xpath) {
			var result = Delete(GetXmlNode(obj), GetXPath(xpath));
			return new Field(XmlNodeType.XmlType, result);
		}

		public static SqlXmlNode Delete(SqlXmlNode node, string xpath) {
			return node.Delete(xpath);
		}

		public static Field Exists(Field obj, Field xpath) {
			return Exists(obj, GetXPath(xpath));
		}

		public static Field Exists(Field obj, string xpath) {
			throw new NotImplementedException();
		}

		public static Field InsertChild(Field obj, Field xpath, Field child, Field value) {
			throw new NotImplementedException();
		}

		public static SqlXmlNode InsertChild(SqlXmlNode node, string xpath, SqlXmlNode child, SqlXmlNode value) {
			return node.InsertChild(xpath, child, value);
		}

		public static Field InsertBefore(Field obj, Field xpath, Field value) {
			return InsertBefore(obj, GetXPath(xpath), value);
		}

		public static Field InsertBefore(Field obj, string xpath, Field value) {
			throw new NotImplementedException();
		}

		public static SqlXmlNode Update(SqlXmlNode node, string xpath, Field value) {
			return node.Update(xpath, value.Value);
		}

		public static Field Update(Field obj, Field xpath, Field value) {
			var result = Update(GetXmlNode(obj), GetXPath(xpath), value);
			return new Field(XmlNodeType.XmlType, result);
		}
	}
}
