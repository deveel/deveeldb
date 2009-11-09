using System;
using System.IO;
using System.Xml.Serialization;

namespace Deveel.Data.DbModel {
	[Serializable]
	public sealed class DbConnectionString {
		private string name;
		private string connectionString;
		private string comment;

		[XmlAttribute("name")]
		public string Name {
			get { return name; }
			set { name = value; }
		}

		[XmlAttribute("connectionString")]
		public string ConnectionString {
			get { return connectionString; }
			set { connectionString = value; }
		}

		[XmlText]
		public string Comment {
			get { return comment; }
			set { comment = value; }
		}

		public override string ToString() {
			return name != null ? name : GetType().FullName;
		}

		public string ToXmlString() {
			StringWriter writer = new StringWriter();
			XmlSerializer serializer = new XmlSerializer(typeof(DbConnectionString));
			serializer.Serialize(writer, this);
			return writer.ToString();
		}

		public static DbConnectionString ParseFromXml(string xmlString) {
			StringReader reader = new StringReader(xmlString);
			XmlSerializer serializer = new XmlSerializer(typeof(DbConnectionString));
			return (DbConnectionString) serializer.Deserialize(reader);
		}
	}
}