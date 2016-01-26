using NUnit.Framework;
using System;
using System.Configuration;
using System.Data.Common;
using System.Collections.Generic;
using System.Threading;
using System.Reflection;
using System.Data;

namespace nunitadonet
{
	[TestFixture ()]
	public partial class Test
	{
		[Test]
		public void T0000_VerifyAppDomainHasConfigurationSettings()
		{
			string value = ConfigurationManager.AppSettings["MyTestConfig"];
			Assert.IsFalse(String.IsNullOrEmpty(value), "No App.Config found.");
		}

		[Test]
		public void T0010_Check_AssemblyQualifiedName()
		{
			Type t = typeof(Deveel.Data.Client.DeveelDbClientFactory);
			string s = t.Assembly.FullName.ToString();
			//Assert.AreEqual (s, "deveeldb, Version=2.0.0.16545, Culture=neutral, PublicKeyToken=null");
		}

		[Test]
		public void T0020_DbProviderFactories_GetFactory()
		{
			var cs = ConfigurationManager.ConnectionStrings["MyConnectionString"];
			var providerName = cs.ProviderName;

			var table = DbProviderFactories.GetFactoryClasses(); // Name, Description, InvariantName, AssemblyQualifiedName
			Assert.IsNotNull (table);

			var Rows = table.Select (string.Format("InvariantName = '{0}'", providerName));
			Assert.AreEqual (Rows.Length, 1);

			var assemblyQualifiedName = Rows [0] ["AssemblyQualifiedName"];
			Assert.IsNotNullOrEmpty (assemblyQualifiedName as string);
			Type t = typeof(Deveel.Data.Client.DeveelDbClientFactory);
			string aqn = t.FullName + ", " + t.Assembly.FullName.ToString ();
			//Assert.AreEqual (aqn, (string)assemblyQualifiedName);

			// see http://www.mono-project.com/docs/advanced/pinvoke/dllnotfoundexception/
			// on how to view load info with MONO_LOG_LEVEL=debug /usr/bin/mono ...
			Type providerType = Type.GetType((string)assemblyQualifiedName);
			Assert.IsNotNull (providerType);

			var factory = DbProviderFactories.GetFactory(providerName); 			
			Assert.IsNotNull (factory);
		}

		[Test]
		public void T0030_Test_FirstCreate_ThenOpen()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);
			//int count = GetTablesCount ();
			//Assert.Equals (0, count);
			OpenDatabase (out con, out tran);
		}

		[Test]
		public void T0040_Test_CheckDatabaseIsEmpty()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);
			//int count = GetTablesCount ();
			//Assert.Equals (0, count);
		}

		[Test]
		public void T0050_CreateSchemaObjects()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);

			//int count = GetTablesCount ();
			//if (count == 0)
			{
				CreateTable(con, tran);
			}
		}

		[Test]
		public void T0060_ListAllTables()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);

			CreateTable (con, tran);

			//tran.Commit ();
			//tran = con.BeginTransaction();

			InsertData (con, tran);

			using (var cmd = con.CreateCommand())
			{
				Assert.IsNotNull (cmd);
				cmd.CommandText = "SELECT * FROM INFORMATION_SCHEMA.TABLES;";
				using (var reader = cmd.ExecuteReader())
				{
					int columnsCount = -1;
					while (reader.Read())
					{
						columnsCount = reader.FieldCount;
						var fields = new List<string>(columnsCount);
						for (int i = 0; i < columnsCount; ++i)
						{
							var o = reader[i];
							var t = reader.GetFieldType(i);
							if (t != typeof(string))
							{
								o = Convert.ChangeType(o, typeof(string), Thread.CurrentThread.CurrentCulture);
							}
							string str = (string)o;
							fields.Add(str);
						}
					}
					reader.Close();
				}
			}
		}

		[Test]
		public void T0070_MultirowInsertWithToDate()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);

			CreateTable (con, tran);

			//tran.Commit ();
			//tran = con.BeginTransaction();

			InsertData (con, tran);
		}

		[Test]
		public void T0071_MultirowInsertWithToDateTime()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);

			CreateTable (con, tran);

			//tran.Commit ();
			//tran = con.BeginTransaction();

			Insert_WithToDateTime(con, tran);
		}

		[Test]
		public void T0080_MultirowInsertWithCast()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);

			CreateTable (con, tran);

			//tran.Commit ();
			//tran = con.BeginTransaction();

			InsertData2(con, tran);
		}
	}
}
