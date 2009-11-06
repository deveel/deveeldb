using System;
using System.Collections;

using Deveel.Shell;

namespace Deveel.Data.Commands {
	internal class MetaProperty {
		static MetaProperty() {
			Types[DbTypes.DB_STRING] = "STRING";
			Types[DbTypes.DB_NUMERIC] = "NUMERIC";
			Types[DbTypes.DB_TIME] = "TIME";
			Types[DbTypes.DB_BOOLEAN] = "BOOLEAN";
			Types[DbTypes.DB_BLOB] = "BLOB";

			SQLTypes2TypeName[SQLTypes.CHAR] = Types[DbTypes.DB_STRING];
			SQLTypes2TypeName[SQLTypes.VARCHAR] = Types[DbTypes.DB_STRING];

			// hope that, 'OTHER' can be read/written as String..
			SQLTypes2TypeName[SQLTypes.OTHER] = Types[DbTypes.DB_STRING];

			SQLTypes2TypeName[SQLTypes.LONGVARBINARY] = Types[DbTypes.DB_BLOB];
			// CLOB not supported .. try string.
			SQLTypes2TypeName[SQLTypes.LONGVARCHAR] = Types[DbTypes.DB_STRING];

			// not supported yet.
			SQLTypes2TypeName[SQLTypes.BLOB] = Types[DbTypes.DB_BLOB];
			// CLOB not supported .. try string.
			SQLTypes2TypeName[SQLTypes.CLOB] = Types[DbTypes.DB_STRING];

			// generic float.
			SQLTypes2TypeName[SQLTypes.DOUBLE] = Types[DbTypes.DB_NUMERIC_EXTENDED];
			SQLTypes2TypeName[SQLTypes.FLOAT] = Types[DbTypes.DB_NUMERIC_EXTENDED];

			// generic numeric. could be integer or double
			SQLTypes2TypeName[SQLTypes.BIGINT] = Types[DbTypes.DB_NUMERIC];
			SQLTypes2TypeName[SQLTypes.NUMERIC] = Types[DbTypes.DB_NUMERIC];
			SQLTypes2TypeName[SQLTypes.DECIMAL] = Types[DbTypes.DB_NUMERIC];
			SQLTypes2TypeName[SQLTypes.BOOLEAN] = Types[DbTypes.DB_NUMERIC];
			// generic integer.
			SQLTypes2TypeName[SQLTypes.INTEGER] = Types[DbTypes.DB_NUMERIC];
			SQLTypes2TypeName[SQLTypes.SMALLINT] = Types[DbTypes.DB_NUMERIC];
			SQLTypes2TypeName[SQLTypes.TINYINT] = Types[DbTypes.DB_NUMERIC];

			SQLTypes2TypeName[SQLTypes.DATE] = Types[DbTypes.DB_TIME];
			SQLTypes2TypeName[SQLTypes.TIME] = Types[DbTypes.DB_TIME];
			SQLTypes2TypeName[SQLTypes.TIMESTAMP] = Types[DbTypes.DB_TIME];
		}

		private int maxLen;
		public readonly String fieldName;
		public DbTypes type;
		public String typeName;

		public static readonly Hashtable Types = new Hashtable();

		private static readonly IDictionary SQLTypes2TypeName = new Hashtable();

		public MetaProperty(String fieldName) {
			this.fieldName = fieldName;
			maxLen = -1;
		}

		public MetaProperty(String fieldName, int sqlType) {
			this.fieldName = fieldName;
			typeName = (String)SQLTypes2TypeName[sqlType];
			if (typeName == null) {
				OutputDevice.Message.WriteLine("cannot handle type '" + type + "' for field '" + this.fieldName +
											   "'; trying String..");
				type = DbTypes.DB_STRING;
				typeName = (string) Types[this.type];
			} else {
				type = findType(typeName);
			}
			maxLen = -1;
		}

		public String FieldName {
			get { return fieldName; }
		}

		public String TypeName {
			get { return typeName; }
			set {
				type = findType(value);
				typeName = value;
			}
		}


		public void updateMaxLength(String val) {
			if (val != null) {
				updateMaxLength(val.Length);
			}
		}

		public void updateMaxLength(int maxLen) {
			if (maxLen > this.maxLen) {
				this.maxLen = maxLen;
			}
		}

		public int MaxLength {
			get { return this.maxLen; }
		}

		/**
		 * find the type in the array. uses linear search, but this is
		 * only a small list.
		 */
		private static DbTypes findType(String typeName) {
			if (typeName == null) {
				throw new ArgumentNullException("typeName");
			}
			typeName = typeName.ToUpper();
			switch (typeName) {
				case "STRING":
					return DbTypes.DB_STRING;
				case "NUMERIC":
				case "DOUBLE":
				case "INTEGER":
					return DbTypes.DB_NUMERIC;
				case "NUMERIC_EXTENDED":
					return DbTypes.DB_NUMERIC_EXTENDED;
				case "BOOLEAN":
					return DbTypes.DB_BOOLEAN;
				case "TIME":
					return DbTypes.DB_TIME;
				case "BLOB":
					return DbTypes.DB_BLOB;
			}

			throw new ArgumentException("invalid type " + typeName);
		}

		public DbTypes Type {
			get { return type; }
		}

		public int renderWidth() {
			return System.Math.Max(typeName.Length, fieldName.Length);
		}
	}
}