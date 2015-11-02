using System;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.XPath;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlXmlNode : ISqlObject, IDisposable {
		private byte[] content;

		public static readonly SqlXmlNode Null = new SqlXmlNode(null, true);

		private SqlXmlNode(byte[] content, bool isNull) {
			this.content = content;
			IsNull = isNull;
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

		private string SelectSingle(string xpath, string xmlNs) {
			XmlNamespaceManager nsManager = null;
			if (!String.IsNullOrEmpty(xmlNs)) {
				var nameTable = new NameTable();
				nameTable.Add(xmlNs);
				nsManager = new XmlNamespaceManager(nameTable);
			}

			using (var stream = new MemoryStream(content)) {
				using (var reader = new StreamReader(stream)) {
					var xpathDoc = new XPathDocument(reader);
					var navigator = xpathDoc.CreateNavigator();
					var singleNodeNavigator = navigator.SelectSingleNode(xpath, nsManager);
					if (singleNodeNavigator == null)
						return null;

					return singleNodeNavigator.Value;
				}
			}
		}

		public SqlXmlNode Extract(string xpath) {
			return Extract(xpath, null);
		}

		public SqlXmlNode Extract(string xpath, string xmlNs) {
			if (IsNull)
				return Null;

			var value = SelectSingle(xpath, xmlNs);
			if (String.IsNullOrEmpty(value))
				return Null;

			// TODO: Control the encoding on properties of the type...
			var bytes = Encoding.UTF8.GetBytes(value);
			return new SqlXmlNode(bytes);
		}

		public SqlXmlNode Update(string xpath, ISqlObject value) {
			return Update(xpath, value, null);
		}

		public SqlXmlNode Update(string xpath, ISqlObject value, string xmlNs) {
			throw new NotImplementedException();
		}

		public override string ToString() {
			if (IsNull)
				return String.Empty;

			// TODO: Control the encoding on properties of the type...
			return Encoding.UTF8.GetString(content);
		}

		public SqlString ToSqlString() {
			if (IsNull)
				return SqlString.Null;

			// TODO: Control the encoding on properties of the type...
			var chars = Encoding.UTF8.GetChars(content);
			return new SqlString(chars);
		}

		public SqlBinary ToSqlBinary() {
			if (IsNull)
				return SqlBinary.Null;

			return new SqlBinary(content);
		}

		public void Dispose() {
			content = null;
		}

		public SqlXmlNode AppendChild(string xpath, SqlXmlNode value) {
			throw new NotImplementedException();
		}

		public SqlXmlNode ExtractValue(SqlXmlNode node, string xpath) {
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
