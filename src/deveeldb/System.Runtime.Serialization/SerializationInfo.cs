using System;
using System.Collections;
using System.Collections.Generic;

namespace System.Runtime.Serialization {
	public sealed class SerializationInfo : IEnumerable<SerializationEntry> {
		private readonly IFormatterConverter converter;
		private readonly Dictionary<string, SerializationEntry> entries;

		private string assemblyName;
		private string typeName;

		[CLSCompliant(false)]
		public SerializationInfo(Type type, IFormatterConverter converter) {
			this.converter = converter;
			if (type == null)
				throw new ArgumentNullException("type");
			if (converter == null)
				throw new ArgumentNullException("converter");

			ObjectType = type;
			assemblyName = type.Assembly.FullName;
			typeName = type.FullName;

			entries = new Dictionary<string, SerializationEntry>();
		}

		public SerializationInfo(Type type)
			: this(type, new FormatterConverter()) {
		}

		public bool IsAssemblyNameSetExplicit { get; private set; }

		public bool IsFullTypeNameSetExplicit { get; private set; }

		public Type ObjectType { get; private set; }

		public string AssemblyName {
			get { return assemblyName; }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");

				assemblyName = value;
				IsAssemblyNameSetExplicit = true;
			}
		}

		public string FullTypeName {
			get { return typeName; }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");

				typeName = value;
				IsFullTypeNameSetExplicit = true;
			}
		}

		public int MemberCount {
			get { return entries.Count; }
		}

		public void SetType(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			typeName = type.FullName;
			assemblyName = type.Assembly.FullName;
			ObjectType = type;
			IsAssemblyNameSetExplicit = false;
			IsFullTypeNameSetExplicit = false;
		}

		public void AddValue(string name, object value, Type type) {
			if (name == null)
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			if (entries.ContainsKey(name))
				throw new SerializationException(String.Format("The member '{0}' is already added.", name));

			entries.Add(name, new SerializationEntry(name, type, value));
		}

		public object GetValue(string name, Type type) {
			if (name == null)
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			SerializationEntry entry;
			if (!entries.TryGetValue(name, out entry))
				throw new SerializationException(String.Format("The member '{0}' was not added.", name));

			var value = entry.Value;

			if (value != null && !type.IsInstanceOfType(value))
				value = converter.Convert(value, type);

			return value;
		}

		IEnumerator<SerializationEntry> IEnumerable<SerializationEntry>.GetEnumerator() {
			return GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public SerializationEnumerator GetEnumerator() {
			return new SerializationEnumerator(entries.Values);
		}

		public void AddValue(string name, object value) {
			Type objType = typeof (object);
			if (value != null)
				objType = value.GetType();

			AddValue(name, value, objType);
		}

		public void AddValue(string name, byte value) {
			AddValue(name, value, typeof(byte));
		}

		[CLSCompliant(false)]
		public void AddValue(string name, sbyte value) {
			AddValue(name, value, typeof(sbyte));
		}

		public void AddValue(string name, bool value) {
			AddValue(name, value, typeof(bool));
		}

		public void AddValue(string name, short value) {
			AddValue(name, value, typeof(short));
		}

		[CLSCompliant(false)]
		public void AddValue(string name, ushort value) {
			AddValue(name, value, typeof(ushort));
		}

		public void AddValue(string name, int value) {
			AddValue(name, value, typeof(int));			
		}

		[CLSCompliant(false)]
		public void AddValue(string name, uint value) {
			AddValue(name, value, typeof(int));
		}

		public void AddValue(string name, long value) {
			AddValue(name, value, typeof(long));
		}

		[CLSCompliant(false)]
		public void AddValue(string name, ulong value) {
			AddValue(name, value, typeof(ulong));
		}

		public void AddValue(string name, float value) {
			AddValue(name, value, typeof(float));
		}

		public void AddValue(string name, double value) {
			AddValue(name, value, typeof(double));			
		}

		public void AddValue(string name, decimal value) {
			AddValue(name, value, typeof(decimal));
		}

		public void AddValue(string name, string value) {
			AddValue(name, value, typeof(string));
		}

		public void AddValue(string name, DateTime value) {
			AddValue(name, value, typeof(DateTime));
		}

		public void AddValue(string name, char value) {
			AddValue(name, value, typeof(char));
		}

		public bool GetBoolean(string name) {
			var value = GetValue(name, typeof (bool));
			return converter.ToBoolean(value);
		}

		public byte GetByte(string name) {
			var value = GetValue(name, typeof (byte));
			return converter.ToByte(value);
		}

		[CLSCompliant(false)]
		public sbyte GetSByte(string name) {
			var value = GetValue(name, typeof (sbyte));
			return converter.ToSByte(value);
		}

		public short GetInt16(string name) {
			var value = GetValue(name, typeof (short));
			return converter.ToInt16(value);
		}

		[CLSCompliant(false)]
		public ushort GetUInt16(string name) {
			var value = GetValue(name, typeof (ushort));
			return converter.ToUInt16(value);
		}

		public int GetInt32(string name) {
			var value = GetValue(name, typeof (int));
			return converter.ToInt32(value);
		}

		[CLSCompliant(false)]
		public uint GetUInt32(string name) {
			var value = GetValue(name, typeof (uint));
			return converter.ToUInt32(value);
		}

		public long GetInt64(string name) {
			var value = GetValue(name, typeof (long));
			return converter.ToInt64(value);
		}

		[CLSCompliant(false)]
		public ulong GetUInt64(string name) {
			var value = GetValue(name, typeof (ulong));
			return converter.ToUInt64(value);
		}

		public float GetSingle(string name) {
			var value = GetValue(name, typeof (float));
			return converter.ToSingle(value);
		}

		public double GetDouble(string name) {
			var value = GetValue(name, typeof (double));
			return converter.ToDouble(value);
		}

		public decimal GetDecimal(string name) {
			var value = GetValue(name, typeof (decimal));
			return converter.ToDecimal(value);
		}

		public char GetChar(string name) {
			var value = GetValue(name, typeof (char));
			return converter.ToChar(value);
		}

		public string GetString(string name) {
			var value = GetValue(name, typeof (string));
			return converter.ToString(value);
		}

		public DateTime GetDateTime(string name) {
			var value = GetValue(name, typeof (DateTime));
			return converter.ToDateTime(value);
		}
	}
}
