using System;

namespace Deveel.Data.Client {
	internal class ColumnInfo {
		private readonly string name;
		private readonly DeveelDbType type;
		private readonly SqlType sqlType;
		private readonly int size;
		private readonly int scale;
		private readonly bool notNull;
		private bool unique;
		private int uniqueGroup;

		public ColumnInfo(string name, int type, SqlType sqlType, int size, int scale, bool notNull) {
			this.name = name;
			this.notNull = notNull;
			this.scale = scale;
			this.size = size;
			this.sqlType = sqlType;
			this.type = ConvertFromDbType(type);
		}

		public int UniqueGroup {
			get { return uniqueGroup; }
		}

		public bool Unique {
			get { return unique; }
		}

		public bool NotNull {
			get { return notNull; }
		}

		public int Scale {
			get { return scale; }
		}

		public int Size {
			get { return size; }
		}

		public DeveelDbType Type {
			get { return type; }
		}

		public SqlType SqlType {
			get { return sqlType; }
		}

		public string Name {
			get { return name; }
		}

		public bool IsQuantifiable {
			get { return type != DeveelDbType.LOB && type != DeveelDbType.UDT; }
		}

		public bool IsNumericType {
			get {
				return type == DeveelDbType.Int4 ||
				       type == DeveelDbType.Int8 ||
				       type == DeveelDbType.Number;
			}
		}

		private static DeveelDbType ConvertFromDbType(int value) {
			switch(value) {
				case -1: case 0: return DeveelDbType.Unknown;
				case 1: return DeveelDbType.String;
				case 2: return DeveelDbType.Number;
				case 3: return DeveelDbType.Time;
				case 5: return DeveelDbType.Boolean;
				case 6: return DeveelDbType.LOB;
				case 7: return DeveelDbType.UDT;
				case 9: return DeveelDbType.Interval;
				default:
					throw new NotSupportedException();
			}
		}

		private static int ConvertToDbType(DeveelDbType value) {
			switch(value) {
				case DeveelDbType.String:
					return 1;
				case DeveelDbType.Int4:
				case DeveelDbType.Int8:
				case DeveelDbType.Number:
					return 2;
				case DeveelDbType.Time:
					return 3;
				case DeveelDbType.Boolean:
					return 5;
				case DeveelDbType.LOB:
					return 6;
				case DeveelDbType.UDT:
					return 7;
				case DeveelDbType.Interval:
					return 8;
				default:
					throw new NotSupportedException();
			}
		}

		public void SetUnique(int group) {
			unique = true;
			uniqueGroup = group;
		}
	}
}