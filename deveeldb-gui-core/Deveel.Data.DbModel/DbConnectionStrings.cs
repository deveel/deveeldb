using System;
using System.Collections;
using System.IO;
using System.Xml.Serialization;

namespace Deveel.Data.DbModel {
	[Serializable]
	public sealed class DbConnectionStrings {
		public DbConnectionStrings() {
			connections = new ArrayList();
		}

		private string defaultConnection;
		private readonly ArrayList connections;

		public DbConnectionString[] Strings {
			get { return (DbConnectionString[]) connections.ToArray(typeof (DbConnectionString)); }
			set {
				connections.Clear();
				connections.AddRange(value);
			}
		}

		[XmlAttribute("default")]
		public string DefaultConnection {
			get { return defaultConnection; }
			set { defaultConnection = value; }
		}

		public void AddConnection(DbConnectionString connectionString) {
			connections.Add(connectionString);
		}

		public void RemoveConnection(DbConnectionString connectionString) {
			connections.Remove(connectionString);
		}

		public string ToXmlString() {
			StringWriter writer = new StringWriter();
			XmlSerializer serializer = new XmlSerializer(typeof(DbConnectionStrings));
			serializer.Serialize(writer, this);
			return writer.ToString();
		}

		public static DbConnectionStrings ParseXmlString(string xmlString) {
			StringReader reader = new StringReader(xmlString);
			XmlSerializer serializer = new XmlSerializer(typeof(DbConnectionStrings));
			return (DbConnectionStrings) serializer.Deserialize(reader);
		}
	}
}