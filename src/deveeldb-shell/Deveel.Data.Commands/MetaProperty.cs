//#if DEBUG
//using System;
//using System.Collections;

//using Deveel.Shell;

//namespace Deveel.Data.Commands {
//    internal class MetaProperty {
//        static MetaProperty() {
//            Types[DbType.String] = "STRING";
//            Types[DbType.Numeric] = "NUMERIC";
//            Types[DbType.Time] = "TIME";
//            Types[DbType.Boolean] = "BOOLEAN";
//            Types[DbType.Blob] = "BLOB";

//            SQLTypes2TypeName[SqlType.Char] = Types[DbType.String];
//            SQLTypes2TypeName[SqlType.VarChar] = Types[DbType.String];

//            // hope that, 'OTHER' can be read/written as String..
//            SQLTypes2TypeName[SqlType.Other] = Types[DbType.String];

//            SQLTypes2TypeName[SqlType.LongVarBinary] = Types[DbType.Blob];
//            // CLOB not supported .. try string.
//            SQLTypes2TypeName[SqlType.LongVarChar] = Types[DbType.String];

//            // not supported yet.
//            SQLTypes2TypeName[SqlType.Blob] = Types[DbType.Blob];
//            // CLOB not supported .. try string.
//            SQLTypes2TypeName[SqlType.Clob] = Types[DbType.String];

//            // generic float.
//            SQLTypes2TypeName[SqlType.Double] = Types[DbType.NumericExtended];
//            SQLTypes2TypeName[SqlType.Float] = Types[DbType.NumericExtended];

//            // generic numeric. could be integer or double
//            SQLTypes2TypeName[SqlType.BigInt] = Types[DbType.Numeric];
//            SQLTypes2TypeName[SqlType.Numeric] = Types[DbType.Numeric];
//            SQLTypes2TypeName[SqlType.Decimal] = Types[DbType.Numeric];
//            SQLTypes2TypeName[SqlType.Boolean] = Types[DbType.Numeric];
//            // generic integer.
//            SQLTypes2TypeName[SqlType.Integer] = Types[DbType.Numeric];
//            SQLTypes2TypeName[SqlType.SmallInt] = Types[DbType.Numeric];
//            SQLTypes2TypeName[SqlType.TinyInt] = Types[DbType.Numeric];

//            SQLTypes2TypeName[SqlType.Date] = Types[DbType.Time];
//            SQLTypes2TypeName[SqlType.Time] = Types[DbType.Time];
//            SQLTypes2TypeName[SqlType.TimeStamp] = Types[DbType.Time];
//        }

//        private int maxLen;
//        public readonly String fieldName;
//        public DbType type;
//        public String typeName;

//        public static readonly Hashtable Types = new Hashtable();

//        private static readonly IDictionary SQLTypes2TypeName = new Hashtable();

//        public MetaProperty(String fieldName) {
//            this.fieldName = fieldName;
//            maxLen = -1;
//        }

//        public MetaProperty(String fieldName, SqlType sqlType) {
//            this.fieldName = fieldName;
//            typeName = (String)SQLTypes2TypeName[sqlType];
//            if (typeName == null) {
//                OutputDevice.Message.WriteLine("cannot handle type '" + type + "' for field '" + this.fieldName +
//                                               "'; trying String..");
//                type = DbType.String;
//                typeName = (string) Types[this.type];
//            } else {
//                type = FindType(typeName);
//            }
//            maxLen = -1;
//        }

//        public String FieldName {
//            get { return fieldName; }
//        }

//        public String TypeName {
//            get { return typeName; }
//            set {
//                type = FindType(value);
//                typeName = value;
//            }
//        }


//        public void UpdateMaxLength(String val) {
//            if (val != null) {
//                UpdateMaxLength(val.Length);
//            }
//        }

//        public void UpdateMaxLength(int value) {
//            if (value > maxLen) {
//                maxLen = value;
//            }
//        }

//        public int MaxLength {
//            get { return maxLen; }
//        }

//        /**
//         * find the type in the array. uses linear search, but this is
//         * only a small list.
//         */
//        private static DbType FindType(String typeName) {
//            if (typeName == null) {
//                throw new ArgumentNullException("typeName");
//            }
//            typeName = typeName.ToUpper();
//            switch (typeName) {
//                case "STRING":
//                    return DbType.String;
//                case "NUMERIC":
//                case "DOUBLE":
//                case "INTEGER":
//                    return DbType.Numeric;
//                case "NUMERIC_EXTENDED":
//                    return DbType.NumericExtended;
//                case "BOOLEAN":
//                    return DbType.Boolean;
//                case "TIME":
//                    return DbType.Time;
//                case "BLOB":
//                    return DbType.Blob;
//            }

//            throw new ArgumentException("invalid type " + typeName);
//        }

//        public DbType Type {
//            get { return type; }
//        }

//        public int RenderWidth {
//            get { return System.Math.Max(typeName.Length, fieldName.Length); }
//        }
//    }
//}
//#endif