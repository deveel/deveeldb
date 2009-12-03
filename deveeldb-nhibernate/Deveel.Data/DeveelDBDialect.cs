using System;
using System.Collections;
using System.Data;

using NHibernate;
using NHibernate.Engine;
using NHibernate.Dialect;
using NHibernate.Dialect.Function;
using NHibernate.SqlCommand;

namespace Deveel.Data {
	public class DeveelDBDialect : Dialect {
		public DeveelDBDialect() {
			RegisterCharacterTypes();
			RegisterNumericTypes();
			RegisterDateTimeTypes();
			RegisterLargeObjectTypes();

			RegisterFunctions();
		}

		public override string AddColumnString {
			get { return "ADD COLUMN"; }
		}

		// DeveelDB supports identities in the form DEFAULT UUNIQUEKEY('TableName'), but
		// it's not clear how to make this in NHibernate dialect...
		public override bool SupportsIdentityColumns {
			get { return base.SupportsIdentityColumns; }
		}

		public override string IdentityColumnString {
			get { return base.IdentityColumnString; }
		}

		public override string CurrentTimestampSQLFunctionName {
			get { return "CURRENT_TIMESTAMP"; }
		}

		public override bool SupportsSequences {
			get { return true; }
		}

		private void RegisterFunctions() {
			RegisterFunction("abs", new StandardSQLFunction("abs"));
			RegisterFunction("sign", new StandardSQLFunction("sign", NHibernateUtil.Int32));

			RegisterFunction("acos", new StandardSQLFunction("acos", NHibernateUtil.Double));
			RegisterFunction("asin", new StandardSQLFunction("asin", NHibernateUtil.Double));
			RegisterFunction("atan", new StandardSQLFunction("atan", NHibernateUtil.Double));
			RegisterFunction("cos", new StandardSQLFunction("cos", NHibernateUtil.Double));
			RegisterFunction("sin", new StandardSQLFunction("sin", NHibernateUtil.Double));
			RegisterFunction("sinh", new StandardSQLFunction("sinh", NHibernateUtil.Double));
			RegisterFunction("tan", new StandardSQLFunction("tan", NHibernateUtil.Double));
			RegisterFunction("tanh", new StandardSQLFunction("tanh", NHibernateUtil.Double));
			RegisterFunction("sqrt", new StandardSQLFunction("sqrt", NHibernateUtil.Double));

			RegisterFunction("round", new StandardSQLFunction("round"));
			RegisterFunction("ceil", new StandardSQLFunction("ceil"));
			RegisterFunction("floor", new StandardSQLFunction("floor"));
			// RegisterFunction("atan2", new StandardSQLFunction("atan2", NHibernateUtil.Single));
			RegisterFunction("log", new StandardSQLFunction("log", NHibernateUtil.Int32));
			RegisterFunction("mod", new StandardSQLFunction("mod", NHibernateUtil.Int32));
			RegisterFunction("pow", new StandardSQLFunction("power", NHibernateUtil.Single));

			RegisterFunction("lower", new StandardSQLFunction("lower"));
			RegisterFunction("ltrim", new StandardSQLFunction("ltrim"));
			RegisterFunction("rtrim", new StandardSQLFunction("rtrim"));
			RegisterFunction("soundex", new StandardSQLFunction("soundex"));
			RegisterFunction("upper", new StandardSQLFunction("upper"));
			RegisterFunction("right", new SQLFunctionTemplate(NHibernateUtil.String, "substr(?1, -?2)"));

			RegisterFunction("current_date", new NoArgSQLFunction("current_date", NHibernateUtil.Date, false));
			RegisterFunction("current_time", new NoArgSQLFunction("current_timestamp", NHibernateUtil.Time, false));
			RegisterFunction("current_timestamp", new CurrentTimeStamp());

			RegisterFunction("user", new NoArgSQLFunction("user", NHibernateUtil.String, false));

			RegisterFunction("substr", new StandardSQLFunction("substr", NHibernateUtil.String));
			RegisterFunction("substring", new StandardSQLFunction("substr", NHibernateUtil.String));
			RegisterFunction("coalesce", new NvlFunction());

			//TODO: continue...
		}

		private void RegisterNumericTypes() {
			RegisterColumnType(System.Data.DbType.Boolean, "NUMBER(1,0)");
			RegisterColumnType(System.Data.DbType.Byte, "NUMBER(3,0)");
			RegisterColumnType(System.Data.DbType.Int16, "NUMBER(5,0)");
			RegisterColumnType(System.Data.DbType.Int32, "NUMBER(10,0)");
			RegisterColumnType(System.Data.DbType.Int64, "NUMBER(20,0)");
			RegisterColumnType(System.Data.DbType.UInt16, "NUMBER(5,0)");
			RegisterColumnType(System.Data.DbType.UInt32, "NUMBER(10,0)");
			RegisterColumnType(System.Data.DbType.UInt64, "NUMBER(20,0)");

			RegisterColumnType(System.Data.DbType.Single, "FLOAT(24)");
			RegisterColumnType(System.Data.DbType.Double, "DOUBLE PRECISION");
			RegisterColumnType(System.Data.DbType.Double, 19, "NUMBER($p,$s)");
			RegisterColumnType(System.Data.DbType.Decimal, "NUMBER(19,5)");
			RegisterColumnType(System.Data.DbType.Decimal, 19, "NUMBER($p,$s)");
		}

		private void RegisterDateTimeTypes() {
			RegisterColumnType(System.Data.DbType.Date, "DATE");
			RegisterColumnType(System.Data.DbType.DateTime, "TIMESTAMP");
			RegisterColumnType(System.Data.DbType.Time, "TIME");
			RegisterColumnType(System.Data.DbType.DateTimeOffset, "INTERVAL");
		}

		private void RegisterCharacterTypes() {
			RegisterColumnType(System.Data.DbType.AnsiStringFixedLength, "CHAR(255)");
			RegisterColumnType(System.Data.DbType.AnsiStringFixedLength, 2000, "CHAR($l)");
			RegisterColumnType(System.Data.DbType.AnsiString, "VARCHAR2(255)");
			RegisterColumnType(System.Data.DbType.AnsiString, 4000, "VARCHAR2($l)");
			RegisterColumnType(System.Data.DbType.StringFixedLength, "CHAR(255)");
			RegisterColumnType(System.Data.DbType.StringFixedLength, 2000, "CHAR($l)");
			RegisterColumnType(System.Data.DbType.String, "VARCHAR2(255)");
			RegisterColumnType(System.Data.DbType.String, 4000, "VARCHAR2($l)");
		}

		private void RegisterLargeObjectTypes() {
			RegisterColumnType(System.Data.DbType.Binary, 2147483647, "BLOB");
			RegisterColumnType(System.Data.DbType.AnsiString, 2147483647, "CLOB");
		}

		public override string GetSelectSequenceNextValString(string sequenceName) {
			return String.Format("nextval ('{0})", sequenceName);
		}

		public override string GetCreateSequenceString(string sequenceName) {
			return "create sequence " + sequenceName;
		}

		public override string GetDropSequenceString(string sequenceName) {
			return "drop sequence " + sequenceName;
		}


		[Serializable]
		private class CurrentTimeStamp : NoArgSQLFunction {
			public CurrentTimeStamp() : base("current_timestamp", NHibernateUtil.DateTime, true) { }

			public override SqlString Render(IList args, ISessionFactoryImplementor factory) {
				return new SqlString(Name);
			}
		}
	}
}