// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

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

			var reader = new BinaryReader(stream, Encoding);
			return Deserialize(reader, graphType);
		}

		public object Deserialize(BinaryReader reader, Type graphType) {
			if (reader == null)
				throw new ArgumentNullException("reader");
			if (graphType == null)
				throw new ArgumentNullException("graphType");

			if (!Attribute.IsDefined(graphType, typeof (SerializableAttribute)))
				throw new ArgumentException(String.Format("The type '{0}' is not marked as serializable.", graphType));

			if (typeof (ISerializable).IsAssignableFrom(graphType))
				return CustomDeserialize(reader, graphType);

			return DeserializeType(reader, graphType);
		}

		private object DeserializeType(BinaryReader reader, Type graphType) {
			var ctor = GetDefaultConstructor(graphType);
			if (ctor == null)
				throw new NotSupportedException(String.Format("The type '{0}' does not specify any default empty constructor.", graphType));

			var fields = graphType.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Where(member => !member.IsDefined(typeof (NonSerializedAttribute), false));
			var properties = graphType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Where(member => member.CanWrite && !member.IsDefined(typeof (NonSerializedAttribute), false));

			var members = new List<MemberInfo>();
			members.AddRange(fields.Cast<MemberInfo>());
			members.AddRange(properties.Cast<MemberInfo>());

			var values = new Dictionary<string, object>();
			ReadValues(reader, Encoding, values);

			var obj = ctor.Invoke(new object[0]);

			foreach (var member in members) {
				var memberName = member.Name;
				object value;

				if (values.TryGetValue(memberName, out value)) {
					// TODO: convert the source value to the destination value...

					if (member is PropertyInfo) {
						var property = (PropertyInfo) member;
						property.SetValue(obj, value, null);
					} else if (member is FieldInfo) {
						var field = (FieldInfo) member;
						field.SetValue(obj, value);
					}
				}
			}

			return obj;
		}

		private ConstructorInfo GetDefaultConstructor(Type type) {
			var ctors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
			foreach (var ctor in ctors) {
				if (ctor.GetParameters().Length == 0)
					return ctor;
			}

			return null;
		}

		private object CustomDeserialize(BinaryReader reader, Type graphType) {
			var ctor = GetSpecialConstructor(graphType);
			if (ctor == null)
				throw new NotSupportedException(String.Format("The type '{0}' has not the special serialization constructor",
					graphType));

			var values = new Dictionary<string, object>();
			ReadValues(reader, Encoding, values);

			var graph = new ObjectData(graphType, values);
			return ctor.Invoke(new object[] {graph});
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
			var nullCheck = reader.ReadBoolean();

			if (nullCheck)
				return null;

			if (typeCode == BooleanType)
				return reader.ReadBoolean();
			if (typeCode == ByteType)
				return reader.ReadByte();
			if (typeCode == Int16Type)
				return reader.ReadInt16();
			if (typeCode == Int32Type)
				return reader.ReadInt32();
			if (typeCode == Int64Type)
				return reader.ReadInt64();
			if (typeCode == SingleType)
				return reader.ReadSingle();
			if (typeCode == DoubleType)
				return reader.ReadDouble();
			if (typeCode == StringType)
				return reader.ReadString();
			if (typeCode == ObjectType)
				return ReadObject(reader, encoding);
			if (typeCode == ArrayType)
				return ReadArray(reader, encoding);

			throw new NotSupportedException("Invalid type code in serialization graph");
		}

		private static Type ReadType(BinaryReader reader) {
			var typeString = reader.ReadString();
			return Type.GetType(typeString, true);
		}

		private static object ReadObject(BinaryReader reader, Encoding encoding) {
			var objType = ReadType(reader);
			var serializer = new BinarySerializer {
				Encoding = encoding
			};

			return serializer.Deserialize(reader, objType);
		}

		private static Array ReadArray(BinaryReader reader, Encoding encoding) {
			var objType = ReadType(reader);
			var arrayLength = reader.ReadInt32();
			var array = Array.CreateInstance(objType, arrayLength);
			for (int i = 0; i < arrayLength; i++) {
				array.SetValue(ReadValue(reader, encoding), i);
			}

			return array;
		}

		private ConstructorInfo GetSpecialConstructor(Type type) {
			var ctors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
			foreach (var ctor in ctors) {
				var paramTypes = ctor.GetParameters().Select(x => x.ParameterType).ToArray();
				if (paramTypes.Length == 1 && paramTypes[0] == typeof(ObjectData))
					return ctor;
			}

			return null;
		}

		public void Serialize(Stream stream, object obj) {
			if (stream == null)
				throw new ArgumentNullException("stream");

			if (!stream.CanWrite)
				throw new ArgumentException("The serialization stream is not writeable.");

			var writer = new BinaryWriter(stream, Encoding);
			Serialize(writer, obj);
		}

		public void Serialize(BinaryWriter writer, object obj) {
			if (writer == null)
				throw new ArgumentNullException("writer");
			if (obj == null)
				throw new ArgumentNullException("obj");

			var objType = obj.GetType();

			if (!Attribute.IsDefined(objType, typeof(SerializableAttribute)))
				throw new ArgumentException(String.Format("The type '{0} is not serializable", objType.FullName));

			var graph = new SerializeData(objType);

			if (typeof (ISerializable).IsAssignableFrom(objType)) {
				((ISerializable) obj).GetData(graph);
			} else {
				GetObjectValues(objType, obj, graph);
			}

			SerializeGraph(writer, Encoding, graph);
		}

		private static void GetObjectValues(Type objType, object obj, SerializeData graph) {
			var fields = objType.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
				.Where(x => !x.IsDefined(typeof (NonSerializedAttribute), false) && !x.Name.EndsWith("_BackingField"));
			var properties = objType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Where(x => !x.IsDefined(typeof (NonSerializedAttribute), false) && x.CanRead);

			var members = new List<MemberInfo>();
			members.AddRange(fields.Cast<MemberInfo>());
			members.AddRange(properties.Cast<MemberInfo>());

			foreach (var member in members) {
				var memberName = member.Name;
				Type memberType;

				object value;
				if (member is FieldInfo) {
					value = ((FieldInfo) member).GetValue(obj);
					memberType = ((FieldInfo) member).FieldType;
				} else if (member is PropertyInfo) {
					value = ((PropertyInfo) member).GetValue(obj, null);
					memberType = ((PropertyInfo) member).PropertyType;
				} else {
					throw new NotSupportedException();
				}

				graph.SetValue(memberName, memberType, value);
			}
		}

		private static void SerializeGraph(BinaryWriter writer, Encoding encoding, SerializeData graph) {
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

		private const byte BooleanType = 1;
		private const byte ByteType = 2;
		private const byte Int16Type = 3;
		private const byte Int32Type = 4;
		private const byte Int64Type = 5;
		private const byte SingleType = 6;
		private const byte DoubleType = 7;
		private const byte StringType = 8;
		private const byte ObjectType = 15;
		private const byte ArrayType = 20;

		private static byte? GetTypeCode(Type type) {
			if (type.IsArray)
				return ArrayType;

			if (type.IsPrimitive) {
				if (type == typeof(bool))
					return BooleanType;
				if (type == typeof(byte))
					return ByteType;
				if (type == typeof(short))
					return Int16Type;
				if (type == typeof(int))
					return Int32Type;
				if (type == typeof(long))
					return Int64Type;
				if (type == typeof(float))
					return SingleType;
				if (type == typeof(double))
					return DoubleType;
			}

			if (type == typeof (string))
				return StringType;

			if (Attribute.IsDefined(type, typeof(SerializableAttribute)))
				return ObjectType;

			return null;
		}

		private static void SerializeValue(BinaryWriter writer, Encoding encoding, Type type, object value) {
			var typeCode = GetTypeCode(type);
			if (typeCode == null)
				throw new NotSupportedException(String.Format("The type '{0}' is not supported.", type));

			var nullCheck = value == null;

			writer.Write(typeCode.Value);
			writer.Write(nullCheck);

			if (value == null)
				return;

			if (typeCode == ArrayType) {
				var typeString = type.GetElementType().FullName;
				writer.Write(typeString);

				var array = (Array) value;
				var arrayLength = array.Length;
				var arrayType = type.GetElementType();

				writer.Write(arrayLength);

				for (int i = 0; i < arrayLength; i++) {
					var element = array.GetValue(i);
					SerializeValue(writer, encoding, arrayType, element);
				}
			} else if (typeCode == ObjectType) {
				var realType = value.GetType();
				writer.Write(realType.FullName);

				var serializer = new BinarySerializer {Encoding = encoding};
				serializer.Serialize(writer, value);
			} else if (typeCode == BooleanType) {
				writer.Write((bool) value);
			} else if (typeCode == ByteType) {
				writer.Write((byte) value);
			} else if (typeCode == Int16Type) {
				writer.Write((short) value);
			} else if (typeCode == Int32Type) {
				writer.Write((int) value);
			} else if (typeCode == Int64Type) {
				writer.Write((long) value);
			} else if (typeCode == SingleType) {
				writer.Write((float) value);
			} else if (typeCode == DoubleType) {
				writer.Write((double) value);
			} else if (typeCode == StringType) {
				writer.Write((string) value);
			}
		}
	}
}
