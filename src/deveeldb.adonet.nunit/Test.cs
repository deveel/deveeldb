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
	public class Test
	{
		[Test]
		public void VerifyAppDomainHasConfigurationSettings()
		{
			string value = ConfigurationManager.AppSettings["MyTestConfig"];
			Assert.IsFalse(String.IsNullOrEmpty(value), "No App.Config found.");
		}

		[Test]
		public void Test_AssemblyQualifiedName()
		{
			Type t = typeof(Deveel.Data.Client.DeveelDbClientFactory);
			string s = t.Assembly.FullName.ToString();
			//Assert.AreEqual (s, "deveeldb, Version=2.0.0.16545, Culture=neutral, PublicKeyToken=null");
		}

		[Test]
		public void Test_DbProviderFactories_GetFactory()
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

		static void CreateDatabase (out IDbConnection con, out IDbTransaction tran)
		{
			var cs = ConfigurationManager.ConnectionStrings ["MyConnectionString"];
			var factory = DbProviderFactories.GetFactory (cs.ProviderName);
			using (con = factory.CreateConnection ()) {
				Assert.IsNotNull (con);
				con.ConnectionString = "Create = true; " + cs.ConnectionString;
				con.Open ();
				using (var cmd = con.CreateCommand ()) {
					Assert.IsNotNull (cmd);
					// tran = con.BeginTransaction ();
					tran = null;
				}
			}
		}

		static void OpenDatabase (out IDbConnection con, out IDbTransaction tran)
		{
			var cs = ConfigurationManager.ConnectionStrings ["MyConnectionString"];
			var factory = DbProviderFactories.GetFactory (cs.ProviderName);
			using (con = factory.CreateConnection ()) {
				Assert.IsNotNull (con);
				con.ConnectionString = cs.ConnectionString;
				con.Open ();
				using (var cmd = con.CreateCommand ()) {
					Assert.IsNotNull (cmd);
					// tran = con.BeginTransaction ();
					tran = null;
				}
			}
		}

		[Test]
		public void Test_FirstCreate_ThenOpen()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);
			//int count = GetTablesCount ();
			//Assert.Equals (0, count);
			OpenDatabase (out con, out tran);
		}

		public int GetTablesCount(IDbConnection con, IDbTransaction tran)
		{
			using (var cmd = con.CreateCommand())
			{
				Assert.IsNotNull (cmd);
				cmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES;";
				var cnt = cmd.ExecuteScalar ();
				Assert.IsNotNull (cnt);
				Assert.IsAssignableFrom<int>(cnt);
				int count = (int)cnt;
				return count;
			}
		}

		static void CreateTable (IDbConnection con, IDbTransaction tran)
		{
			using (var cmd = con.CreateCommand ()) {
				Assert.IsNotNull (cmd);
				cmd.CommandText = "CREATE TABLE mytable (myident INTEGER, mystamp TIMESTAMP, mycontent VARCHAR(50));";
				int affected = cmd.ExecuteNonQuery ();
				Assert.AreEqual (0, affected);
			}
		}

		static void InsertData (IDbConnection con, IDbTransaction tran)
		{
			using (var cmd = con.CreateCommand ()) {
				Assert.IsNotNull (cmd);
				cmd.CommandText = "INSERT INTO mytable (myident, mystamp, mycontent) VALUES (1, '2016-01-01', '2 weeks ago'), (2, '2016-01-07', '1 week ago');";
				int affected = cmd.ExecuteNonQuery ();
				Assert.AreEqual (2, affected);
			}
		}

		[Test]
		public void Test_CheckDatabaseIsEmpty()
		{
			IDbConnection con;
			IDbTransaction tran;
			CreateDatabase (out con, out tran);
			//int count = GetTablesCount ();
			//Assert.Equals (0, count);
		}

		[Test]
		public void Test_CreateSchemaObjects()
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
		public void Test_MultirowInsert()
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
		public void Test_ListAllTables()
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
	}
}
