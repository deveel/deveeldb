using System;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.XPath;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlXmlNode : ISqlObject {
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

		public SqlXmlNode SelectSingle(string xpath) {
			return SelectSingle(xpath, null);
		}

		public SqlXmlNode SelectSingle(string xpath, string xmlNs) {
			if (IsNull)
				return Null;

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
						return Null;

					// TODO: Let the encoding be defined elsewhere
					var nodeContent = Encoding.UTF8.GetBytes(singleNodeNavigator.Value);
					return new SqlXmlNode(nodeContent);
				}
			}
		}
	}
}
