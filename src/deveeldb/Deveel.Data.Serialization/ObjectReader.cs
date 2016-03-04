using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;

namespace Deveel.Data.Serialization {
	class ObjectReader {
		ISurrogateSelector _surrogateSelector;
		StreamingContext _context;
		SerializationBinder _binder;

		TypeFilterLevel _filterLevel;

		ObjectManager _manager;
		Dictionary<long, string> _registeredAssemblies = new Dictionary<long, string>();
		Dictionary<Type, TypeMetadata> _typeMetadataCache = new Dictionary<Type, TypeMetadata>();

		object _lastObject = null;
		long _lastObjectID = 0;
		long _rootObjectID = 0;
		byte[] arrayBuffer;
		int ArrayBufferLength = 4096;

		public ObjectReader(BinaryFormatter formatter) {
			_surrogateSelector = formatter.SurrogateSelector;
			_context = formatter.Context;
			_binder = formatter.Binder;
			_manager = new ObjectManager(_surrogateSelector, _context);

			_filterLevel = formatter.FilterLevel;
		}

		public object CurrentObject {
			get { return _lastObject; }
		}

		bool ReadNextObject(BinaryElementType element, BinaryReader reader) {
			if (element == BinaryElementType.End) {
				_manager.DoFixups();

				_manager.RaiseDeserializationEvent();
				return false;
			}

			SerializationInfo info;
			long objectId;

			ReadObject(element, reader, out objectId, out _lastObject, out info);

			if (objectId != 0) {
				RegisterObject(objectId, _lastObject, info, 0, null, null);
				_lastObjectID = objectId;
			}

			return true;
		}

		private void ReadObject(BinaryElementType element, BinaryReader reader, out long objectId, out object value, out SerializationInfo info) {
			switch (element) {
				//case BinaryElementType.RefTypeObject:
				//	ReadRefTypeObjectInstance(reader, out objectId, out value, out info);
				//	break;

				//case BinaryElementType.UntypedRuntimeObject:
				//	ReadObjectInstance(reader, true, false, out objectId, out value, out info);
				//	break;

				//case BinaryElementType.UntypedExternalObject:
				//	ReadObjectInstance(reader, false, false, out objectId, out value, out info);
				//	break;

				//case BinaryElementType.RuntimeObject:
				//	ReadObjectInstance(reader, true, true, out objectId, out value, out info);
				//	break;

				//case BinaryElementType.ExternalObject:
				//	ReadObjectInstance(reader, false, true, out objectId, out value, out info);
				//	break;

				case BinaryElementType.String:
					info = null;
					ReadStringIntance(reader, out objectId, out value);
					break;

				case BinaryElementType.GenericArray:
					info = null;
					ReadGenericArray(reader, out objectId, out value);
					break;


				case BinaryElementType.BoxedPrimitiveTypeValue:
					value = ReadBoxedPrimitiveTypeValue(reader);
					objectId = 0;
					info = null;
					break;

				case BinaryElementType.NullValue:
					value = null;
					objectId = 0;
					info = null;
					break;

				case BinaryElementType.Assembly:
					ReadAssembly(reader);
					ReadObject((BinaryElementType)reader.ReadByte(), reader, out objectId, out value, out info);
					break;

				case BinaryElementType.ArrayFiller8b:
					value = new ArrayNullFiller(reader.ReadByte());
					objectId = 0;
					info = null;
					break;

				case BinaryElementType.ArrayFiller32b:
					value = new ArrayNullFiller(reader.ReadInt32());
					objectId = 0;
					info = null;
					break;

				case BinaryElementType.ArrayOfPrimitiveType:
					ReadArrayOfPrimitiveType(reader, out objectId, out value);
					info = null;
					break;

				case BinaryElementType.ArrayOfObject:
					ReadArrayOfObject(reader, out objectId, out value);
					info = null;
					break;

				case BinaryElementType.ArrayOfString:
					ReadArrayOfString(reader, out objectId, out value);
					info = null;
					break;

				default:
					throw new SerializationException("Unexpected binary element: " + (int)element);
			}
		}

		private void ReadAssembly(BinaryReader reader) {
			long id = (long)reader.ReadUInt32();
			string assemblyName = reader.ReadString();
			_registeredAssemblies[id] = assemblyName;
		}

		//private void ReadObjectInstance(BinaryReader reader, bool isRuntimeObject, bool hasTypeInfo, out long objectId, out object value, out SerializationInfo info) {
		//	objectId = (long)reader.ReadUInt32();

		//	TypeMetadata metadata = ReadTypeMetadata(reader, isRuntimeObject, hasTypeInfo);
		//	ReadObjectContent(reader, metadata, objectId, out value, out info);
		//}

		//private void ReadRefTypeObjectInstance(BinaryReader reader, out long objectId, out object value, out SerializationInfo info) {
		//	objectId = (long)reader.ReadUInt32();
		//	long refTypeObjectId = (long)reader.ReadUInt32();

		//	// Gets the type of the referred object and its metadata

		//	object refObj = _manager.GetObject(refTypeObjectId);
		//	if (refObj == null) throw new SerializationException("Invalid binary format");
		//	TypeMetadata metadata = (TypeMetadata)_typeMetadataCache[refObj.GetType()];

		//	ReadObjectContent(reader, metadata, objectId, out value, out info);
		//}

		//private void ReadObjectContent(BinaryReader reader, TypeMetadata metadata, long objectId, out object objectInstance, out SerializationInfo info) {
		//	if (_filterLevel == TypeFilterLevel.Low)
		//		objectInstance = FormatterServices.GetSafeUninitializedObject(metadata.Type);
		//	else
		//		objectInstance = FormatterServices.GetUninitializedObject(metadata.Type);
		//	_manager.RaiseOnDeserializingEvent(objectInstance);

		//	info = metadata.NeedsSerializationInfo ? new SerializationInfo(metadata.Type, new FormatterConverter()) : null;

		//	if (metadata.MemberNames != null)
		//		for (int n = 0; n < metadata.FieldCount; n++)
		//			ReadValue(reader, objectInstance, objectId, info, metadata.MemberTypes[n], metadata.MemberNames[n], null, null);
		//	else
		//		for (int n = 0; n < metadata.FieldCount; n++)
		//			if (metadata.MemberInfos[n] != null)
		//				ReadValue(reader, objectInstance, objectId, info, metadata.MemberTypes[n], metadata.MemberInfos[n].Name, metadata.MemberInfos[n], null);
		//}

		private void RegisterObject(long objectId, object objectInstance, SerializationInfo info, long parentObjectId, MemberInfo parentObjectMemeber, int[] indices) {
			if (parentObjectId == 0) indices = null;

			if (!objectInstance.GetType().IsValueType || parentObjectId == 0)
				_manager.RegisterObject(objectInstance, objectId, info, 0, null, null);
			else {
				if (indices != null) indices = (int[])indices.Clone();
				_manager.RegisterObject(objectInstance, objectId, info, parentObjectId, parentObjectMemeber, indices);
			}
		}

		private void ReadStringIntance(BinaryReader reader, out long objectId, out object value) {
			objectId = (long)reader.ReadUInt32();
			value = reader.ReadString();
		}

		private void ReadGenericArray(BinaryReader reader, out long objectId, out object val) {
			objectId = (long)reader.ReadUInt32();
			// Array structure
			reader.ReadByte();

			int rank = reader.ReadInt32();

			bool emptyDim = false;
			int[] lengths = new int[rank];
			for (int n = 0; n < rank; n++) {
				lengths[n] = reader.ReadInt32();
				if (lengths[n] == 0) emptyDim = true;
			}

			TypeTag code = (TypeTag)reader.ReadByte();
			Type elementType = ReadType(reader, code);

			Array array = Array.CreateInstance(elementType, lengths);

			if (emptyDim) {
				val = array;
				return;
			}

			int[] indices = new int[rank];

			// Initialize indexes
			for (int dim = rank - 1; dim >= 0; dim--)
				indices[dim] = array.GetLowerBound(dim);

			bool end = false;
			while (!end) {
				ReadValue(reader, array, objectId, null, elementType, null, null, indices);

				for (int dim = array.Rank - 1; dim >= 0; dim--) {
					indices[dim]++;
					if (indices[dim] > array.GetUpperBound(dim)) {
						if (dim > 0) {
							indices[dim] = array.GetLowerBound(dim);
							continue;   // Increment the next dimension's index
						}
						end = true; // That was the last dimension. Finished.
					}
					break;
				}
			}
			val = array;
		}

		private object ReadBoxedPrimitiveTypeValue(BinaryReader reader) {
			Type type = ReadType(reader, TypeTag.PrimitiveType);
			return ReadPrimitiveTypeValue(reader, type);
		}

		private void ReadArrayOfPrimitiveType(BinaryReader reader, out long objectId, out object val) {
			objectId = (long)reader.ReadUInt32();
			int length = reader.ReadInt32();
			Type elementType = ReadType(reader, TypeTag.PrimitiveType);

			switch (Type.GetTypeCode(elementType)) {
				case TypeCode.Boolean: {
						bool[] arr = new bool[length];
						for (int n = 0; n < length; n++) arr[n] = reader.ReadBoolean();
						val = arr;
						break;
					}

				case TypeCode.Byte: {
						byte[] arr = new byte[length];
						int pos = 0;
						while (pos < length) {
							int nr = reader.Read(arr, pos, length - pos);
							if (nr == 0) break;
							pos += nr;
						}
						val = arr;
						break;
					}

				case TypeCode.Char: {
						char[] arr = new char[length];
						int pos = 0;
						while (pos < length) {
							int nr = reader.Read(arr, pos, length - pos);
							if (nr == 0) break;
							pos += nr;
						}
						val = arr;
						break;
					}

				case TypeCode.DateTime: {
						DateTime[] arr = new DateTime[length];
						for (int n = 0; n < length; n++) {
							arr[n] = DateTime.FromBinary(reader.ReadInt64());
						}
						val = arr;
						break;
					}
				case TypeCode.Decimal: {
#if !PCL
						Decimal[] arr = new Decimal[length];
						for (int n = 0; n < length; n++) arr[n] = reader.ReadDecimal();
						val = arr;
						break;
#else
					throw new NotSupportedException();
#endif
					}
				case TypeCode.Double: {
						Double[] arr = new Double[length];
						if (length > 2)
							BlockRead(reader, arr, 8);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadDouble();
						val = arr;
						break;
					}

				case TypeCode.Int16: {
						short[] arr = new short[length];
						if (length > 2)
							BlockRead(reader, arr, 2);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadInt16();
						val = arr;
						break;
					}

				case TypeCode.Int32: {
						int[] arr = new int[length];
						if (length > 2)
							BlockRead(reader, arr, 4);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadInt32();
						val = arr;
						break;
					}

				case TypeCode.Int64: {
						long[] arr = new long[length];
						if (length > 2)
							BlockRead(reader, arr, 8);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadInt64();
						val = arr;
						break;
					}

				case TypeCode.SByte: {
						sbyte[] arr = new sbyte[length];
						if (length > 2)
							BlockRead(reader, arr, 1);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadSByte();
						val = arr;
						break;
					}

				case TypeCode.Single: {
						float[] arr = new float[length];
						if (length > 2)
							BlockRead(reader, arr, 4);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadSingle();
						val = arr;
						break;
					}

				case TypeCode.UInt16: {
						ushort[] arr = new ushort[length];
						if (length > 2)
							BlockRead(reader, arr, 2);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadUInt16();
						val = arr;
						break;
					}

				case TypeCode.UInt32: {
						uint[] arr = new uint[length];
						if (length > 2)
							BlockRead(reader, arr, 4);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadUInt32();
						val = arr;
						break;
					}

				case TypeCode.UInt64: {
						ulong[] arr = new ulong[length];
						if (length > 2)
							BlockRead(reader, arr, 8);
						else
							for (int n = 0; n < length; n++) arr[n] = reader.ReadUInt64();
						val = arr;
						break;
					}

				case TypeCode.String: {
						string[] arr = new string[length];
						for (int n = 0; n < length; n++) arr[n] = reader.ReadString();
						val = arr;
						break;
					}

				default: {
						if (elementType == typeof(TimeSpan)) {
							TimeSpan[] arr = new TimeSpan[length];
							for (int n = 0; n < length; n++) arr[n] = new TimeSpan(reader.ReadInt64());
							val = arr;
						} else
							throw new NotSupportedException("Unsupported primitive type: " + elementType.FullName);
						break;
					}
			}
		}

		private void BlockRead(BinaryReader reader, Array array, int dataSize) {
			int totalSize = Buffer.ByteLength(array);

			if (arrayBuffer == null || (totalSize > arrayBuffer.Length && arrayBuffer.Length != ArrayBufferLength))
				arrayBuffer = new byte[totalSize <= ArrayBufferLength ? totalSize : ArrayBufferLength];

			int pos = 0;
			while (totalSize > 0) {
				int size = totalSize < arrayBuffer.Length ? totalSize : arrayBuffer.Length;
				int ap = 0;
				do {
					int nr = reader.Read(arrayBuffer, ap, size - ap);
					if (nr == 0) break;
					ap += nr;
				} while (ap < size);

				if (!BitConverter.IsLittleEndian && dataSize > 1)
					BinaryCommon.SwapBytes(arrayBuffer, size, dataSize);

				Buffer.BlockCopy(arrayBuffer, 0, array, pos, size);
				totalSize -= size;
				pos += size;
			}
		}

		private void ReadArrayOfObject(BinaryReader reader, out long objectId, out object array) {
			ReadSimpleArray(reader, typeof(object), out objectId, out array);
		}

		private void ReadArrayOfString(BinaryReader reader, out long objectId, out object array) {
			ReadSimpleArray(reader, typeof(string), out objectId, out array);
		}

		private void ReadSimpleArray(BinaryReader reader, Type elementType, out long objectId, out object val) {
			objectId = (long)reader.ReadUInt32();
			int length = reader.ReadInt32();
			int[] indices = new int[1];

			Array array = Array.CreateInstance(elementType, length);
			for (int n = 0; n < length; n++) {
				indices[0] = n;
				ReadValue(reader, array, objectId, null, elementType, null, null, indices);
				n = indices[0];
			}
			val = array;
		}

		private TypeMetadata ReadTypeMetadata(BinaryReader reader, bool isRuntimeObject, bool hasTypeInfo) {
			TypeMetadata metadata = new TypeMetadata();

			string className = reader.ReadString();
			int fieldCount = reader.ReadInt32();

			Type[] types = new Type[fieldCount];
			string[] names = new string[fieldCount];

			for (int n = 0; n < fieldCount; n++)
				names[n] = reader.ReadString();

			if (hasTypeInfo) {
				TypeTag[] codes = new TypeTag[fieldCount];

				for (int n = 0; n < fieldCount; n++)
					codes[n] = (TypeTag)reader.ReadByte();

				for (int n = 0; n < fieldCount; n++) {
					Type t = ReadType(reader, codes[n], false);
					// The field's type could not be resolved: assume it is an object.
					if (t == null)
						t = typeof(object);
					types[n] = t;
				}
			}

			// Gets the type

			if (!isRuntimeObject) {
				long assemblyId = (long)reader.ReadUInt32();
				metadata.Type = GetDeserializationType(assemblyId, className);
			} else
				metadata.Type = Type.GetType(className, true);

			metadata.MemberTypes = types;
			metadata.MemberNames = names;
			metadata.FieldCount = names.Length;

			// Now check if this objects needs a SerializationInfo struct for deserialziation.
			// SerializationInfo is needed if the object has to be deserialized using
			// a serialization surrogate, or if it implements ISerializable.

			if (_surrogateSelector != null) {
				// check if the surrogate selector handles objects of the given type. 
				ISurrogateSelector selector;
				ISerializationSurrogate surrogate = _surrogateSelector.GetSurrogate(metadata.Type, _context, out selector);
				metadata.NeedsSerializationInfo = (surrogate != null);
			}

			if (!metadata.NeedsSerializationInfo) {
				// Check if the object is marked with the Serializable attribute

#if PCL
				if (!Attribute.IsDefined(metadata.Type, typeof(SerializableAttribute)))
#else
				if (!metadata.Type.IsSerializable)
#endif
					throw new SerializationException("Serializable objects must be marked with the Serializable attribute");

				metadata.NeedsSerializationInfo = typeof(ISerializable).IsAssignableFrom(metadata.Type);
				if (!metadata.NeedsSerializationInfo) {
					metadata.MemberInfos = new MemberInfo[fieldCount];
					for (int n = 0; n < fieldCount; n++) {
						FieldInfo field = null;
						string memberName = names[n];

						int i = memberName.IndexOf('+');
						if (i != -1) {
							string baseTypeName = names[n].Substring(0, i);
							memberName = names[n].Substring(i + 1);
							Type t = metadata.Type.BaseType;
							while (t != null) {
								if (t.Name == baseTypeName) {
									field = t.GetField(memberName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
									break;
								} else
									t = t.BaseType;
							}
						} else
							field = metadata.Type.GetField(memberName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

						if (field != null)
							metadata.MemberInfos[n] = field;
#if ONLY_1_1
						else
							throw new SerializationException ("Field \"" + names[n] + "\" not found in class " + metadata.Type.FullName);
#endif

						if (!hasTypeInfo) {
							types[n] = field.FieldType;
						}
					}
					metadata.MemberNames = null;    // Info now in MemberInfos
				}
			}

			// Registers the type's metadata so it can be reused later if
			// a RefTypeObject element is found

			if (!_typeMetadataCache.ContainsKey(metadata.Type))
				_typeMetadataCache[metadata.Type] = metadata;

			return metadata;
		}

		static bool IsGeneric(MemberInfo minfo) {
			if (minfo == null)
				return false;

			if (!(minfo is FieldInfo))
				throw new NotSupportedException("Not supported: " + minfo.GetType());

			Type mtype = ((FieldInfo)minfo).FieldType;
			return (mtype.IsGenericType);
		}

		private void ReadValue(BinaryReader reader, object parentObject, long parentObjectId, SerializationInfo info, Type valueType, string fieldName, MemberInfo memberInfo, int[] indices) {
			// Reads a value from the stream and assigns it to the member of an object

			object val;

			if (BinaryCommon.IsPrimitive(valueType) && !IsGeneric(memberInfo)) {
				val = ReadPrimitiveTypeValue(reader, valueType);
				SetObjectValue(parentObject, fieldName, memberInfo, info, val, valueType, indices);
				return;
			}

			// Gets the object

			BinaryElementType element = (BinaryElementType)reader.ReadByte();

			if (element == BinaryElementType.ObjectReference) {
				// Just read the id of the referred object and record a fixup
				long childObjectId = (long)reader.ReadUInt32();
				RecordFixup(parentObjectId, childObjectId, parentObject, info, fieldName, memberInfo, indices);
				return;
			}

			long objectId;
			SerializationInfo objectInfo;

			ReadObject(element, reader, out objectId, out val, out objectInfo);

			// There are two cases where the object cannot be assigned to the parent
			// and a fixup must be used:
			//  1) When what has been read is not an object, but an id of an object that
			//     has not been read yet (an object reference). This is managed in the
			//     previous block of code.
			//  2) When the read object is a value type object. Value type fields hold
			//     copies of objects, not references. Thus, if the value object that
			//     has been read has pending fixups, those fixups would be made to the
			//     boxed copy in the ObjectManager, and not in the required object instance

			// First of all register the fixup, and then the object. ObjectManager is more
			// efficient if done in this order

			bool hasFixup = false;
			if (objectId != 0) {
				if (val.GetType().IsValueType) {
					RecordFixup(parentObjectId, objectId, parentObject, info, fieldName, memberInfo, indices);
					hasFixup = true;
				}

				// Register the value

				if (info == null && !(parentObject is Array))
					RegisterObject(objectId, val, objectInfo, parentObjectId, memberInfo, null);
				else
					RegisterObject(objectId, val, objectInfo, parentObjectId, null, indices);
			}
			// Assign the value to the parent object, unless there is a fixup

			if (!hasFixup)
				SetObjectValue(parentObject, fieldName, memberInfo, info, val, valueType, indices);
		}

		private void SetObjectValue(object parentObject, string fieldName, MemberInfo memberInfo, SerializationInfo info, object value, Type valueType, int[] indices) {
			if (value is IObjectReference)
				value = ((IObjectReference)value).GetRealObject(_context);

			if (parentObject is Array) {
				if (value is ArrayNullFiller) {
					// It must be a single dimension array of objects.
					// Just increase the index. Elements are null by default.
					int count = ((ArrayNullFiller)value).NullCount;
					indices[0] += count - 1;
				} else
					((Array)parentObject).SetValue(value, indices);
			} else if (info != null) {
				info.AddValue(fieldName, value, valueType);
			} else {
				if (memberInfo is FieldInfo)
					((FieldInfo)memberInfo).SetValue(parentObject, value);
				else
					((PropertyInfo)memberInfo).SetValue(parentObject, value, null);
			}
		}

		private void RecordFixup(long parentObjectId, long childObjectId, object parentObject, SerializationInfo info, string fieldName, MemberInfo memberInfo, int[] indices) {
			if (info != null) {
				_manager.RecordDelayedFixup(parentObjectId, fieldName, childObjectId);
			} else if (parentObject is Array) {
				if (indices.Length == 1)
					_manager.RecordArrayElementFixup(parentObjectId, indices[0], childObjectId);
				else
					_manager.RecordArrayElementFixup(parentObjectId, (int[])indices.Clone(), childObjectId);
			} else {
				_manager.RecordFixup(parentObjectId, memberInfo, childObjectId);
			}
		}

		private Type GetDeserializationType(long assemblyId, string className) {
			return GetDeserializationType(assemblyId, className, true);
		}

		private Type GetDeserializationType(long assemblyId, string className, bool throwOnError) {
			Type t;
			string assemblyName = (string)_registeredAssemblies[assemblyId];

			if (_binder != null) {
				t = _binder.BindToType(assemblyName, className);
				if (t != null)
					return t;
			}

			Assembly assembly;
			try {
				assembly = Assembly.Load(assemblyName);
			} catch (Exception ex) {
				if (!throwOnError)
					return null;
				throw new SerializationException(String.Format("Couldn't find assembly '{0}'", assemblyName), ex);
			}

			t = assembly.GetType(className);
			if (t != null)
				return t;

			if (!throwOnError)
				return null;

			throw new SerializationException(String.Format("Couldn't find type '{0}' in assembly '{1}'", className, assemblyName));
		}

		public Type ReadType(BinaryReader reader, TypeTag code) {
			return ReadType(reader, code, true);
		}

		public Type ReadType(BinaryReader reader, TypeTag code, bool throwOnError) {
			switch (code) {
				case TypeTag.PrimitiveType:
					return BinaryCommon.GetTypeFromCode(reader.ReadByte());

				case TypeTag.String:
					return typeof(string);

				case TypeTag.ObjectType:
					return typeof(object);

				case TypeTag.RuntimeType: {
						string name = reader.ReadString();
						// map MS.NET's System.RuntimeType to System.MonoType
						if (_context.State == StreamingContextStates.Remoting)
							throw new NotSupportedException();

						Type t = Type.GetType(name);
						if (t != null)
							return t;

						throw new SerializationException(String.Format("Could not find type '{0}'.", name));
					}

				case TypeTag.GenericType: {
						string name = reader.ReadString();
						long asmid = (long)reader.ReadUInt32();
						return GetDeserializationType(asmid, name, throwOnError);
					}

				case TypeTag.ArrayOfObject:
					return typeof(object[]);

				case TypeTag.ArrayOfString:
					return typeof(string[]);

				case TypeTag.ArrayOfPrimitiveType:
					Type elementType = BinaryCommon.GetTypeFromCode(reader.ReadByte());
					return Type.GetType(elementType.FullName + "[]");

				default:
					throw new NotSupportedException("Unknow type tag");
			}
		}

		public static object ReadPrimitiveTypeValue(BinaryReader reader, Type type) {
			if (type == null) return null;

			switch (Type.GetTypeCode(type)) {
				case TypeCode.Boolean:
					return reader.ReadBoolean();

				case TypeCode.Byte:
					return reader.ReadByte();

				case TypeCode.Char:
					return reader.ReadChar();

				case TypeCode.DateTime:
					return DateTime.FromBinary(reader.ReadInt64());

				case TypeCode.Decimal:
					return Decimal.Parse(reader.ReadString(), CultureInfo.InvariantCulture);

				case TypeCode.Double:
					return reader.ReadDouble();

				case TypeCode.Int16:
					return reader.ReadInt16();

				case TypeCode.Int32:
					return reader.ReadInt32();

				case TypeCode.Int64:
					return reader.ReadInt64();

				case TypeCode.SByte:
					return reader.ReadSByte();

				case TypeCode.Single:
					return reader.ReadSingle();

				case TypeCode.UInt16:
					return reader.ReadUInt16();

				case TypeCode.UInt32:
					return reader.ReadUInt32();

				case TypeCode.UInt64:
					return reader.ReadUInt64();

				case TypeCode.String:
					return reader.ReadString();

				default:
					if (type == typeof(TimeSpan))
						return new TimeSpan(reader.ReadInt64());
					else
						throw new NotSupportedException("Unsupported primitive type: " + type.FullName);
			}
		}

		public bool ReadNextObject(BinaryReader reader) {
			var element = (BinaryElementType)reader.ReadByte();
			if (element == BinaryElementType.End) {
				_manager.DoFixups();

				_manager.RaiseDeserializationEvent();
				return false;
			}

			SerializationInfo info;
			long objectId;

			ReadObject(element, reader, out objectId, out _lastObject, out info);

			if (objectId != 0) {
				RegisterObject(objectId, _lastObject, info, 0, null, null);
				_lastObjectID = objectId;
			}

			return true;
		}

		public void ReadObjectGraph(BinaryReader reader, bool readHeaders, out object result, out Header[] headers) {
			var elem = (BinaryElementType)reader.ReadByte();
			ReadObjectGraph(elem, reader, readHeaders, out result, out headers);
		}

		public void ReadObjectGraph(BinaryElementType elem, BinaryReader reader, bool readHeaders, out object result, out Header[] headers) {
			headers = null;

			// Reads the objects. The first object in the stream is the
			// root object.
			bool next = ReadNextObject(elem, reader);
			if (next) {
				do {
					if (readHeaders && (headers == null))
						headers = (Header[])CurrentObject;
					else
						if (_rootObjectID == 0) _rootObjectID = _lastObjectID;
				} while (ReadNextObject(reader));
			}

			result = _manager.GetObject(_rootObjectID);
		}

#region TypeMetadata

		class TypeMetadata {
			public Type Type;
			public Type[] MemberTypes;
			public string[] MemberNames;
			public MemberInfo[] MemberInfos;
			public int FieldCount;
			public bool NeedsSerializationInfo;
		}

#endregion

#region ArrayNullFiller

		class ArrayNullFiller {
			public ArrayNullFiller(int count) {
				NullCount = count;
			}

			public int NullCount;
		}

#endregion
	}
}
