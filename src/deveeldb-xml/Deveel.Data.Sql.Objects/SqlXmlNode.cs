using System;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.XPath;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlXmlNode : ISqlObject, IDisposable {
		private byte[] content;
		private XPathNavigator navigator;

		public static readonly SqlXmlNode Null = new SqlXmlNode(null, true);

		private SqlXmlNode(byte[] content, bool isNull) {
			this.content = content;
			IsNull = isNull;

			navigator = CreateNavigator();
		}

		public SqlXmlNode(byte[] content)
			: this(content, false) {
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull { get; private set; }

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		private XPathNavigator CreateNavigator() {
			if (IsNull)
				return null;

			using (var stream = new MemoryStream(content)) {
				using (var xmlReader = new StreamReader(stream, Encoding.Unicode)) {
					var xmlDocument = new XmlDocument();
					xmlDocument.Load(xmlReader);
					return xmlDocument.CreateNavigator();
				}
			}
		}

		private XmlNamespaceManager NamespaceManager(string xmlNs) {
			XmlNamespaceManager nsManager = null;
			if (!String.IsNullOrEmpty(xmlNs)) {
				var nameTable = new NameTable();
				nameTable.Add(xmlNs);
				nsManager = new XmlNamespaceManager(nameTable);
			}

			return nsManager;
		}

		private void AsserCanEdit() {
			if (!navigator.CanEdit)
				throw new NotSupportedException("The current node cannot be edited.");
		}

		private byte[] SelectSingle(string xpath, string xmlNs) {
			var nodeNavigator = navigator.SelectSingleNode(xpath, NamespaceManager(xmlNs));
			if (nodeNavigator == null)
				return null;

			return AsBinary(nodeNavigator);
		}

		private static byte[] AsBinary(XPathNavigator navigator) {
			using (var stream = new MemoryStream()) {
				using (var xmlWriter = new XmlTextWriter(stream, Encoding.Unicode)) {
					xmlWriter.Formatting = Formatting.None;

					navigator.WriteSubtree(xmlWriter);

					xmlWriter.Flush();

					return stream.ToArray();
				}
			}
		}

		private bool SelectSingleValue(string xpath, string xmlNs, out object value, out Type valueType) {
			value = null;
			valueType = null;

			var nodeNavigator = navigator.SelectSingleNode(xpath, NamespaceManager(xmlNs));
			if (nodeNavigator == null)
				return false;

			valueType = nodeNavigator.ValueType;
			value = nodeNavigator.TypedValue;
			return true;
		}

		private bool Update(string xpath, object value, string xmlNs, out byte[] updated) {
			AsserCanEdit();

			updated = null;

			var rootNavigator = navigator.Clone();
			var nodeNavigator = rootNavigator.SelectSingleNode(xpath, NamespaceManager(xmlNs));
			if (nodeNavigator == null)
				return false;

			nodeNavigator.SetValue(value.ToString());

			updated = AsBinary(rootNavigator);
			return true;
		}

		public SqlXmlNode Extract(string xpath) {
			return Extract(xpath, null);
		}

		public SqlXmlNode Extract(string xpath, string xmlNs) {
			if (IsNull)
				return Null;

			var bytes = SelectSingle(xpath, xmlNs);
			if (bytes == null)
				return Null;

			// TODO: Control the encoding on properties of the type...
			return new SqlXmlNode(bytes);
		}

		public ISqlObject ExtractValue(string xpath) {
			return ExtractValue(xpath, null);
		}

		public ISqlObject ExtractValue(string xpath, string xmlNs) {
			if (IsNull)
				return Null;

			object value;
			Type valueType;
			if (!SelectSingleValue(xpath, xmlNs, out value, out valueType))
				return SqlNull.Value;

			if (valueType == typeof(string))
				return new SqlString((string)value);

			// TODO: support other types

			throw new NotSupportedException();
		}

		public SqlXmlNode Update(string xpath, ISqlObject value) {
			return Update(xpath, value, null);
		}

		public SqlXmlNode Update(string xpath, ISqlObject value, string xmlNs) {
			byte[] updated;
			if (!Update(xpath, value, xmlNs, out updated))
				return Null;

			return new SqlXmlNode(updated);
		}

		public SqlString ToSqlString() {
			if (IsNull)
				return SqlString.Null;

			var chars = Encoding.Unicode.GetChars(content);
			return new SqlString(chars);
		}

		public SqlBinary ToSqlBinary() {
			if (IsNull)
				return SqlBinary.Null;

			return new SqlBinary(content);
		}

		public void Dispose() {
			navigator = null;
			content = null;
		}

		public SqlXmlNode AppendChild(string xpath, SqlXmlNode value) {
			throw new NotImplementedException();
		}

		public SqlXmlNode Delete(string xpath) {
			throw new NotImplementedException();
		}

		public XmlNode ToXmlNode() {
			throw new NotImplementedException();
		}

		public byte[] ToBytes() {
			return content;
		}

		public SqlXmlNode InsertChild(string xpath, SqlXmlNode child, SqlXmlNode value) {
			throw new NotImplementedException();
		}
	}
}
