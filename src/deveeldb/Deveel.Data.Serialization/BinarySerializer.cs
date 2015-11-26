using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Deveel.Data.Types;

namespace Deveel.Data.Serialization {
	public sealed class BinarySerializer {
		public BinarySerializer() {
			Encoding = Encoding.Unicode;
		}

		public Encoding Encoding { get; set; }

		public object Deserialize(Stream stream, Type graphType) {
			if (stream == null)
				throw new ArgumentNullException("stream");
			if (!stream.CanRead)
				throw new ArgumentException("The input stream cannot be read.", "stream");

			using (var reader = new BinaryReader(stream, Encoding)) {
				return Deserialize(reader, graphType);
			}
		}

		public object Deserialize(BinaryReader reader, Type graphType) {
			if (reader == null)
				throw new ArgumentNullException("reader");
			if (graphType == null)
				throw new ArgumentNullException("graphType");

			if (!typeof(ISerializable).IsAssignableFrom(graphType))
				throw new ArgumentException(String.Format("The type '{0}' is not serializable.", graphType));

			var ctor = GetSpecialConstructor(graphType);
			if (ctor == null)
				throw new NotSupportedException(String.Format("The type '{0}' has not the special serialization constructor",
					graphType));

			var values = new Dictionary<string, object>();

			ReadValues(reader, Encoding, values);

			var graph = new SerializedGraph(graphType, values);
			return ctor.Invoke(null, new object[] {graph});
		}

		private static void ReadValues(BinaryReader reader, Encoding encoding, IDictionary<string, object> values) {
			int count = reader.ReadInt32();

			for (int i = 0; i < count; i++) {
				var keyLen = reader.ReadInt32();
				var keyChars = reader.ReadChars(keyLen);
				var key = new string(keyChars);

				var value = ReadValue(reader, encoding);

				values[key] = value;
			}
		}

		private static object ReadValue(BinaryReader reader, Encoding encoding) {
			var typeCode = reader.ReadByte();
			if (typeCode == (byte) TypeCode.Boolean)
				return reader.ReadBoolean();
			if (typeCode == (byte) TypeCode.Byte)
				return reader.ReadByte();
			if (typeCode == (byte) TypeCode.Int16)
				return reader.ReadInt16();
			if (typeCode == (byte)TypeCode.Int32)
				return reader.ReadInt32();
			if (typeCode == (byte) TypeCode.Single)
				return reader.ReadSingle();
			if (typeCode == (byte) TypeCode.Double)
				return reader.ReadDouble();
			if (typeCode == (byte) TypeCode.String)
				return reader.ReadString();
			if (typeCode == (byte) TypeCode.Object)
				return ReadObject(reader, encoding);
			if (typeCode == (byte) TypeCode.DBNull)
				return null;

			throw new NotSupportedException("Invalid type code in serialization graph");
		}

		private static Type ReadType(BinaryReader reader) {
			var typeString = reader.ReadString();
			return Type.GetType(typeString, true, true);
		}

		private static object ReadObject(BinaryReader reader, Encoding encoding) {
			var objTypeCode = reader.ReadByte();
			var objType = ReadType(reader);
			if (objTypeCode == 2) {
				var arrayLength = reader.ReadInt32();
				var array = Array.CreateInstance(objType, arrayLength);
				for (int i = 0; i < arrayLength; i++) {
					array.SetValue(ReadValue(reader, encoding), i);
				}

				return array;
			}

			var serializer = new BinarySerializer {
				Encoding = encoding
			};

			return serializer.Deserialize(reader, objType);
		}

		private ConstructorInfo GetSpecialConstructor(Type type) {
			var argTypes = new[] {typeof (SerializedGraph)};
			return type.GetConstructor(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance, null, argTypes,
				null);
		}

		public void Serialize(Stream stream, object obj) {
			if (stream == null)
				throw new ArgumentNullException("stream");

			if (!stream.CanWrite)
				throw new ArgumentException("The serialization stream is not writeable.");

			using (var writer = new BinaryWriter(stream, Encoding)) {
				Serialize(writer, obj);
			}
		}

		public void Serialize(BinaryWriter writer, object obj) {
			if (writer == null)
				throw new ArgumentNullException("writer");
			if (obj == null)
				throw new ArgumentNullException("obj");

			var objType = obj.GetType();
			if (!typeof(ISerializable).IsAssignableFrom(objType))
				throw new ArgumentException(String.Format("The type '{0} is not serializable", objType.FullName));
			
			var graph = new SerializationGraph(objType);
			((ISerializable)obj).WriteToGraph(graph);

			SerializeGraph(writer, Encoding, graph);
		}

		private static void SerializeGraph(BinaryWriter writer, Encoding encoding, SerializationGraph graph) {
			var values = graph.Values.ToDictionary(x => x.Key, x => x.Value);
			var count = values.Count;

			writer.Write(count);

			foreach (var pair in values) {
				var key = pair.Key;
				var keyLength = key.Length;

				writer.Write(keyLength);
				writer.Write(key.ToCharArray());

				SerializeValue(writer, encoding, pair.Value.Key, pair.Value.Value);
			}
		}

		private static TypeCode GetTypeCode(Type type) {
			if (type.IsPrimitive) {
				if (type == typeof(ushort) ||
					type == typeof(uint) ||
					type == typeof(ulong) ||
					type == typeof(sbyte))
					return TypeCode.Empty;

				if (type == typeof(bool))
					return TypeCode.Boolean;
				if (type == typeof(byte))
					return TypeCode.Byte;
				if (type == typeof(short))
					return TypeCode.Int16;
				if (type == typeof(int))
					return TypeCode.Int32;
				if (type == typeof(long))
					return TypeCode.Int64;
				if (type == typeof(float))
					return TypeCode.Single;
				if (type == typeof(double))
					return TypeCode.Double;
				if (type == typeof(string))
					return TypeCode.String;
			}

			if (!typeof(ISerializable).IsAssignableFrom(type))
				return TypeCode.Empty;

			return TypeCode.Object;
		}

		private static void SerializeValue(BinaryWriter writer, Encoding encoding, Type type, object value) {
			var typeCode = GetTypeCode(type);
			if (typeCode == TypeCode.Empty)
				throw new NotSupportedException(String.Format("The type '{0}' is not supported.", type));

			writer.Write((byte)typeCode);

			if (typeCode == TypeCode.Object) {
				var typeString = type.FullName;
				writer.Write(typeString);

				if (type.IsArray) {
					writer.Write((byte) 2);

					var array = (Array) value;
					var arrayLength = array.Length;
					var arrayType = type.GetElementType();

					writer.Write(arrayLength);

					for (int i = 0; i < arrayLength; i++) {
						var element = array.GetValue(i);
						SerializeValue(writer, encoding, arrayType, element);
					}
				} else {
					writer.Write((byte) 1);
					var serializer = new BinarySerializer {Encoding = encoding};
					serializer.Serialize(writer, value);
				}
			} else if (typeCode == TypeCode.Boolean) {
				writer.Write((bool)value);
			} else if (typeCode == TypeCode.Byte) {
				writer.Write((byte)value);
			} else if (typeCode == TypeCode.Int16) {
				writer.Write((short)value);
			} else if (typeCode == TypeCode.Int32) {
				writer.Write((int)value);
			} else if (typeCode == TypeCode.Int64) {
				writer.Write((long)value);
			} else if (typeCode == TypeCode.Single) {
				writer.Write((float)value);
			} else if (typeCode == TypeCode.Double) {
				writer.Write((double)value);
			} else if (typeCode == TypeCode.String) {
				writer.Write((string)value);
			}
		}
	}
}
