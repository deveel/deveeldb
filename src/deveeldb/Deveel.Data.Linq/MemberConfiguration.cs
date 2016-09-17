using System;
using System.Reflection;

using Deveel.Data.Mapping;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq {
	public sealed class MemberConfiguration : IMemberConfiguration {
		internal MemberConfiguration(MemberInfo member) {
			Member = member;
			IsNullable = true;

			if (member is PropertyInfo) {
				MemberType = ((PropertyInfo) member).PropertyType;
			} else if (member is FieldInfo) {
				MemberType = ((FieldInfo) member).FieldType;
			}
		}

		internal MemberInfo Member { get; private set; }

		private Type MemberType { get; set; }

		private string ColumnName { get; set; }

		private int? ColumnSize { get; set; }

		private bool MaxSize { get; set; }

		private bool IsNullable { get; set; }

		private SqlTypeCode? ColumnTypeCode { get; set; }

		public MemberConfiguration HasColumnName(string columnName) {
			ColumnName = columnName;
			return this;
		}

		public MemberConfiguration HasSize(int value) {
			ColumnSize = value;
			return this;
		}

		public MemberConfiguration IsMaxSize(bool value = true) {
			MaxSize = value;
			if (value)
				ColumnSize = null;
			return this;
		}

		public MemberConfiguration Nullable(bool value = true) {
			IsNullable = value;
			return this;
		}

		public MemberConfiguration OfType(SqlTypeCode typeCode) {
			ColumnTypeCode = typeCode;
			return this;
		}

		DbColumnModel IMemberConfiguration.CreateModel(bool isKey, KeyType keyType) {
			var columnName = DiscoverColumnName();
			var columnType = DiscoverColumnType();
			var model = new DbColumnModel(Member, columnName, columnType) {
				IsKey = isKey,
				KeyType = keyType,
				IsNullable = IsNullable
			};
			return model;
		}

		private string DiscoverColumnName() {
			string name = null;
			if (!String.IsNullOrEmpty(ColumnName)) {
				name = ColumnName;
			} else if (Attribute.IsDefined(Member, typeof(ColumnAttribute))) {
				var attr = (ColumnAttribute) Attribute.GetCustomAttribute(Member, typeof(ColumnAttribute));
				if (!String.IsNullOrEmpty(attr.Name))
					name = attr.Name;
			}

			if (String.IsNullOrEmpty(name))
				name = Member.Name;

			return name;
		}

		private SqlType DiscoverColumnType() {
			SqlTypeCode typeCode;
			if (ColumnTypeCode != null) {
				typeCode = ColumnTypeCode.Value;
			} else {
				typeCode = DiscoverColumnTypeCode();
			}

			if (!PrimitiveTypes.IsPrimitive(typeCode))
				throw new NotSupportedException(String.Format("The type '{0}' is not supported in this context.", typeCode));

			switch (typeCode) {
				case SqlTypeCode.Bit:
				case SqlTypeCode.Boolean:
					return PrimitiveTypes.Boolean(typeCode);
				case SqlTypeCode.TinyInt:
				case SqlTypeCode.SmallInt:
				case SqlTypeCode.Integer:
				case SqlTypeCode.BigInt:
					return PrimitiveTypes.Numeric(typeCode);
				case SqlTypeCode.Decimal:
				case SqlTypeCode.Numeric:
				case SqlTypeCode.Double:
				case SqlTypeCode.Float:
				case SqlTypeCode.Real: {
					int precision;
					if (MaxSize) {
						precision = NumericType.GetPrecision(typeCode);
					} else if (ColumnSize != null) {
						precision = ColumnSize.Value;
					} else {
						precision = -1;
					}
					
					return PrimitiveTypes.Numeric(typeCode, precision);
				}
				case SqlTypeCode.String:
				case SqlTypeCode.VarChar:
				case SqlTypeCode.Char: {
					int maxSize;
					if (MaxSize) {
						maxSize = StringType.DefaultMaxSize;
					} else if (ColumnSize != null) {
						maxSize = ColumnSize.Value;
					} else {
						maxSize = -1;
					}

					return PrimitiveTypes.String(typeCode, maxSize);
				}
				case SqlTypeCode.Time:
				case SqlTypeCode.TimeStamp:
				case SqlTypeCode.Date:
				case SqlTypeCode.DateTime:
					return PrimitiveTypes.DateTime(typeCode);
				case SqlTypeCode.VarBinary:
				case SqlTypeCode.Binary: {
					int maxSize;
					if (MaxSize) {
						maxSize = BinaryType.DefaultMaxSize;
					} else if (ColumnSize != null) {
						maxSize = ColumnSize.Value;
					} else {
						maxSize = -1;
					}

					return PrimitiveTypes.Binary(typeCode, maxSize);
				}
				default:
					throw new NotSupportedException();
			}
		}

		private SqlTypeCode DiscoverColumnTypeCode() {
			if (MemberType == typeof(bool))
				return SqlTypeCode.Boolean;
			if (MemberType == typeof(byte))
				return SqlTypeCode.TinyInt;
			if (MemberType == typeof(short))
				return SqlTypeCode.SmallInt;
			if (MemberType == typeof(int))
				return SqlTypeCode.Integer;
			if (MemberType == typeof(long))
				return SqlTypeCode.BigInt;
			if (MemberType == typeof(float))
				return SqlTypeCode.Real;
			if (MemberType == typeof(double))
				return SqlTypeCode.Double;
			if (MemberType == typeof(string))
				return SqlTypeCode.String;
			if (MemberType == typeof(DateTime) ||
				MemberType == typeof(DateTimeOffset))
				return SqlTypeCode.TimeStamp;
			if (MemberType == typeof(TimeSpan))
				return SqlTypeCode.DayToSecond;
			if (MemberType == typeof(byte[]))
				return SqlTypeCode.Binary;

			throw new NotSupportedException(String.Format("The type '{0}' of member '{1}' is not supported as column type.",
				MemberType, Member.Name));
		}
	}
}
