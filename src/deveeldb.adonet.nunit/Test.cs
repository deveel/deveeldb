using NUnit.Framework;
using System;
using System.Configuration;
using System.Data.Common;
using System.Collections.Generic;
using System.Threading;
using System.Reflection;

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

		public int GetTablesCount()
		{
			var cs = ConfigurationManager.ConnectionStrings["MyConnectionString"];
			var providerName = cs.ProviderName;
			var factory = DbProviderFactories.GetFactory(providerName); 			
			Assert.IsNotNull (factory);
			var connectionString = cs.ConnectionString;
			using (var con = factory.CreateConnection ())
			{
				Assert.IsNotNull (con);
				con.ConnectionString = connectionString;
				con.Open ();
				using (var cmd = con.CreateCommand())
				{
					Assert.IsNotNull (cmd);
					cmd.CommandText = "SELECT COUNT * FROM INFORMATION_SCHEMA.TABLES;";
					var cnt = cmd.ExecuteScalar ();
					Assert.IsNotNull (cnt);
					Assert.IsAssignableFrom<int>(cnt);
					con.Close();
					int count = (int)cnt;
					return count;
				}
			}
		}
		[Test]
		public void Test_CheckDatabaseIsEmpty()
		{
			int count = GetTablesCount ();
			Assert.Equals (0, count);
		}

		[Test]
		public void Test_ListAllTables()
		{
			var cs = ConfigurationManager.ConnectionStrings["MyConnectionString"];
			var providerName = cs.ProviderName;
			var factory = DbProviderFactories.GetFactory(providerName); 			
			Assert.IsNotNull (factory);
			var connectionString = cs.ConnectionString;
			using (var con = factory.CreateConnection ())
			{
				Assert.IsNotNull (con);
				con.ConnectionString = connectionString;
				con.Open ();
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
					con.Close();
				}
			}
		}
	}
}
