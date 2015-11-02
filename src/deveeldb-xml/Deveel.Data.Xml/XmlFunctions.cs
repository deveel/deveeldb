using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;
using Deveel.Data.Types;

namespace Deveel.Data.Xml {
	public static class XmlFunctions {
		static XmlFunctions() {
			Resolver = new XmlFunctionProvider();
		}

		public static IRoutineResolver Resolver { get; private set; }

		private static string GetXPath(DataObject xpath) {
			if (!(xpath.Type is StringType))
				throw new ArgumentException();

			return xpath.AsVarChar().Value.ToString();
		}

		private static SqlXmlNode GetXmlNode(DataObject node) {
			if (!(node.Type is XmlNodeType))
				throw new ArgumentException();

			return node.Value as SqlXmlNode;
		}

		public static DataObject XmlType(DataObject obj) {
			return obj.CastTo(XmlNodeType.XmlType);
		}

		public static DataObject AppendChild(DataObject obj, DataObject xpath, DataObject value) {
			var result = AppendChild(GetXmlNode(obj), GetXPath(xpath), GetXmlNode(value));
			return new DataObject(XmlNodeType.XmlType, result);
		}


		public static SqlXmlNode AppendChild(SqlXmlNode node, string xpath, SqlXmlNode value) {
			return node.AppendChild(xpath, value);
		}

		public static DataObject Extract(DataObject obj, DataObject xpath) {
			var result = Extract(GetXmlNode(obj), GetXPath(xpath));
			return new DataObject(XmlNodeType.XmlType, result);
		}

		public static SqlXmlNode Extract(SqlXmlNode node, string xpath) {
			return node.Extract(xpath);
		}

		public static DataObject ExtractValue(DataObject obj, DataObject xpath) {
			var result = ExtractValue(GetXmlNode(obj), GetXPath(xpath));
			return new DataObject(XmlNodeType.XmlType, result);
		}

		public static SqlXmlNode ExtractValue(SqlXmlNode node, string xpath) {
			return node.ExtractValue(node, xpath);
		}

		public static DataObject Delete(DataObject obj, DataObject xpath) {
			var result = Delete(GetXmlNode(obj), GetXPath(xpath));
			return new DataObject(XmlNodeType.XmlType, result);
		}

		public static SqlXmlNode Delete(SqlXmlNode node, string xpath) {
			return node.Delete(xpath);
		}

		public static DataObject Exists(DataObject obj, DataObject xpath) {
			return Exists(obj, GetXPath(xpath));
		}

		public static DataObject Exists(DataObject obj, string xpath) {
			throw new NotImplementedException();
		}

		public static DataObject InsertChild(DataObject obj, DataObject xpath, DataObject child, DataObject value) {
			throw new NotImplementedException();
		}

		public static SqlXmlNode InsertChild(SqlXmlNode node, string xpath, SqlXmlNode child, SqlXmlNode value) {
			return node.InsertChild(xpath, child, value);
		}

		public static DataObject InsertBefore(DataObject obj, DataObject xpath, DataObject value) {
			return InsertBefore(obj, GetXPath(xpath), value);
		}

		public static DataObject InsertBefore(DataObject obj, string xpath, DataObject value) {
			throw new NotImplementedException();
		}

		public static DataObject Update(DataObject obj, string xpath, DataObject value) {
			throw new NotImplementedException();
		}

		public static DataObject Update(DataObject obj, DataObject xpath, DataObject value) {
			return Update(obj, GetXPath(xpath), value);
		}
	}
}
