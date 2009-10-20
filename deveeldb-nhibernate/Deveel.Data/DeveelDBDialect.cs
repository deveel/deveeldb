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

			//TODO: Soon...
			/*
			RegisterFunction("acos", new StandardSQLFunction("acos", NHibernateUtil.Double));
			RegisterFunction("asin", new StandardSQLFunction("asin", NHibernateUtil.Double));
			RegisterFunction("atan", new StandardSQLFunction("atan", NHibernateUtil.Double));
			RegisterFunction("cos", new StandardSQLFunction("cos", NHibernateUtil.Double));
			RegisterFunction("sin", new StandardSQLFunction("sin", NHibernateUtil.Double));
			RegisterFunction("sinh", new StandardSQLFunction("sinh", NHibernateUtil.Double));
			RegisterFunction("tan", new StandardSQLFunction("tan", NHibernateUtil.Double));
			RegisterFunction("tanh", new StandardSQLFunction("tanh", NHibernateUtil.Double));
			*/
			RegisterFunction("sqrt", new StandardSQLFunction("sqrt", NHibernateUtil.Double));

			// TODO: Soon...
			/*
			RegisterFunction("round", new StandardSQLFunction("round"));
			RegisterFunction("ceil", new StandardSQLFunction("ceil"));
			RegisterFunction("floor", new StandardSQLFunction("floor"));
			RegisterFunction("atan2", new StandardSQLFunction("atan2", NHibernateUtil.Single));
			RegisterFunction("log", new StandardSQLFunction("log", NHibernateUtil.Int32));
			RegisterFunction("mod", new StandardSQLFunction("mod", NHibernateUtil.Int32));
			RegisterFunction("power", new StandardSQLFunction("power", NHibernateUtil.Single));
			*/

			RegisterFunction("lower", new StandardSQLFunction("lower"));
			RegisterFunction("ltrim", new StandardSQLFunction("ltrim"));
			RegisterFunction("rtrim", new StandardSQLFunction("rtrim"));
			// TODO: Soon...
			/*
			RegisterFunction("soundex", new StandardSQLFunction("soundex"));
			*/
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
			RegisterColumnType(DbType.Boolean, "NUMBER(1,0)");
			RegisterColumnType(DbType.Byte, "NUMBER(3,0)");
			RegisterColumnType(DbType.Int16, "NUMBER(5,0)");
			RegisterColumnType(DbType.Int32, "NUMBER(10,0)");
			RegisterColumnType(DbType.Int64, "NUMBER(20,0)");
			RegisterColumnType(DbType.UInt16, "NUMBER(5,0)");
			RegisterColumnType(DbType.UInt32, "NUMBER(10,0)");
			RegisterColumnType(DbType.UInt64, "NUMBER(20,0)");

			RegisterColumnType(DbType.Single, "FLOAT(24)");
			RegisterColumnType(DbType.Double, "DOUBLE PRECISION");
			RegisterColumnType(DbType.Double, 19, "NUMBER($p,$s)");
			RegisterColumnType(DbType.Decimal, "NUMBER(19,5)");
			RegisterColumnType(DbType.Decimal, 19, "NUMBER($p,$s)");
		}

		private void RegisterDateTimeTypes() {
			RegisterColumnType(DbType.Date, "DATE");
			RegisterColumnType(DbType.DateTime, "TIMESTAMP");
			RegisterColumnType(DbType.Time, "TIME");
			//TODO: interval type...
		}

		private void RegisterCharacterTypes() {
			RegisterColumnType(DbType.AnsiStringFixedLength, "CHAR(255)");
			RegisterColumnType(DbType.AnsiStringFixedLength, 2000, "CHAR($l)");
			RegisterColumnType(DbType.AnsiString, "VARCHAR2(255)");
			RegisterColumnType(DbType.AnsiString, 4000, "VARCHAR2($l)");
			RegisterColumnType(DbType.StringFixedLength, "CHAR(255)");
			RegisterColumnType(DbType.StringFixedLength, 2000, "CHAR($l)");
			RegisterColumnType(DbType.String, "VARCHAR2(255)");
			RegisterColumnType(DbType.String, 4000, "VARCHAR2($l)");
		}

		private void RegisterLargeObjectTypes() {
			RegisterColumnType(DbType.Binary, 2147483647, "BLOB");
			RegisterColumnType(DbType.AnsiString, 2147483647, "CLOB");
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