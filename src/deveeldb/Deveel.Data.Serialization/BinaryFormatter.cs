using System;
using System.IO;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Text;

namespace Deveel.Data.Serialization {
	public sealed class BinaryFormatter : IFormatter {
		public BinaryFormatter(ISurrogateSelector surrogateSelector, StreamingContext context) {
			Encoding = Encoding.Unicode;
			Context = context;
			SurrogateSelector = surrogateSelector;
		}

		public BinaryFormatter()
			: this(null, new StreamingContext(StreamingContextStates.All)) {
		}

		public SerializationBinder Binder { get; set; }

		public StreamingContext Context { get; set; }

		public ISurrogateSelector SurrogateSelector { get; set; }

		public Encoding Encoding { get; set; }

		public TypeFilterLevel FilterLevel { get; set; }

		private void ReadBinaryHeader(BinaryReader reader, out bool hasHeaders) {
			reader.ReadByte();
			reader.ReadInt32();
			int val = reader.ReadInt32();
			hasHeaders = (val == 2);
			reader.ReadInt32();
			reader.ReadInt32();
		}

		public object Deserialize(Stream serializationStream) {
			return NoCheckDeserialize(serializationStream, null);
		}

		public object Deserialize(Stream serializationStream, HeaderHandler handler) {
			return NoCheckDeserialize(serializationStream, handler);
		}

		// shared by Deserialize and UnsafeDeserialize which both involve different security checks
		private object NoCheckDeserialize(Stream serializationStream, HeaderHandler handler) {
			if (serializationStream == null) {
				throw new ArgumentNullException("serializationStream");
			}
			if (serializationStream.CanSeek &&
			    serializationStream.Length == 0) {
				throw new SerializationException("serializationStream supports seeking, but its length is 0");
			}

			BinaryReader reader = new BinaryReader(serializationStream);

			bool hasHeader;
			ReadBinaryHeader(reader, out hasHeader);

			// Messages are read using a special static method, which does not use ObjectReader
			// if it is not needed. This saves time and memory.

			var elem = (BinaryElementType) reader.Read();

			var serializer = new ObjectReader(this);

			object result;
			Header[] headers;
			serializer.ReadObjectGraph(elem, reader, hasHeader, out result, out headers);
			if (handler != null)
				handler(headers);

			return result;
		}


		public void Serialize(Stream serializationStream, object graph) {
			throw new NotImplementedException();
		}
	}
}
