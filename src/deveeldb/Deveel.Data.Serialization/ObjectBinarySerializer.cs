using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Serialization {
	public abstract class ObjectBinarySerializer<T> : IObjectBinarySerializer {
		protected ObjectBinarySerializer() 
			: this(Encoding.Unicode) {
		}

		protected ObjectBinarySerializer(Encoding encoding) {
			if (encoding == null)
				throw new ArgumentNullException("encoding");

			Encoding = encoding;
		}

		protected virtual Encoding Encoding { get; private set; }

		void IObjectSerializer.Serialize(object obj, Stream outputStream) {
			using (var writer = new BinaryWriter(outputStream)) {
				Serialize((T)obj, writer);
			}
		}

		object IObjectSerializer.Deserialize(Stream inputStream) {
			using (var reader = new BinaryReader(inputStream)) {
				return Deserialize(reader);
			}
		}

		void IObjectBinarySerializer.Serialize(object obj, BinaryWriter writer) {
			Serialize((T)obj, writer);
		}

		object IObjectBinarySerializer.Deserialize(BinaryReader reader) {
			return Deserialize(reader);
		}

		public abstract void Serialize(T obj, BinaryWriter writer);

		public abstract T Deserialize(BinaryReader reader);
	}
}
