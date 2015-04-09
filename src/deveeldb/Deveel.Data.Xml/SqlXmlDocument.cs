#if !COMPACT
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Xml {
	public sealed class SqlXmlDocument : IXmlNode, ISqlObject {
		private readonly byte[] content;
		private IXmlElement root;

		private SqlXmlDocument(byte[] content) {
			this.content = content;
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull {
			get { return content == null; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		string IXmlNode.Prefix {
			get { return null; }
		}

		string IXmlNode.LocalName {
			get { return null; }
		}

		XmlNodeType IXmlNode.NodeType {
			get { return XmlNodeType.Document; }
		}

		public IXmlElement Root {
			get {
				if (root == null)
					root = LoadRoot();

				return root;
			}
		}

		private IXmlElement LoadRoot() {
			using (var stream = new MemoryStream(content)) {
				using (var reader = new StreamReader(stream, Encoding.UTF8)) {
					using (var xmlReader = new XmlTextReader(reader)) {
						if (!xmlReader.Read())
							return null;

						throw new NotImplementedException();
					}
				}
			}
		}

		public IXmlNode FindSingleNode(string xpath) {
			throw new NotImplementedException();
		}

		public IEnumerable<IXmlNode> FindNodes(string xpath) {
			var result = new List<IXmlNode>();

			using (var stream = new MemoryStream(content)) {
				using (var input = new StreamReader(stream)) {
					using (var reader = new XmlTextReader(input)) {
						var xpathDoc = new XPathDocument(reader);
						var xpathNav = xpathDoc.CreateNavigator();
						var iter = xpathNav.Select(xpath);
						while (iter.MoveNext()) {
						}
					}
				}
			}

			return result.AsReadOnly();
		}

		#region SqlXmlNode

		abstract class SqlXmlNode : IXmlNode {
			protected SqlXmlNode(string prefix, string localName) {
				Prefix = prefix;
				LocalName = localName;
			}

			public string Prefix { get; private set; }

			public string LocalName { get; private set; }

			public abstract XmlNodeType NodeType { get; }

			public IXmlNode FindSingleNode(string xpath) {
				throw new NotImplementedException();
			}

			public IEnumerable<IXmlNode> FindNodes(string xpath) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region SqlXmlElement

		class SqlXmlElement : SqlXmlNode, IXmlElement { 
			public SqlXmlElement(string prefix, string localName) 
				: base(prefix, localName) {
				Attributes = new List<SqlXmlAttribute>();
			}

			public override XmlNodeType NodeType {
				get { return XmlNodeType.Element; }
			}

			public ICollection<SqlXmlAttribute> Attributes { get; private set; }
				
			IEnumerable<IXmlAttribute> IXmlElement.Attributes {
				get { return Attributes.Cast<IXmlAttribute>().AsEnumerable(); }
			}

			public IEnumerable<IXmlNode> Descendants() {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region SqlXmlAttribute

		class SqlXmlAttribute : SqlXmlNode, IXmlAttribute {
			public SqlXmlAttribute(string prefix, string localName, string value) 
				: base(prefix, localName) {
				Value = value;
			}

			public override XmlNodeType NodeType {
				get { return XmlNodeType.Attribute; }
			}

			public string Value { get; private set; }
		}

		#endregion

		#region SqlXmlText

		class SqlXmlText : SqlXmlNode, IXmlText {
			private readonly XmlNodeType nodeType;

			public SqlXmlText(string prefix, string localName, XmlNodeType nodeType, string content) 
				: base(prefix, localName) {
				this.nodeType = nodeType;
				Content = content;
			}

			public override XmlNodeType NodeType {
				get { return nodeType; }
			}

			public string Content { get; private set; }
		}

		#endregion
	}
}
#endif