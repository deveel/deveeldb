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

		[Test ()]
		public void Test_DbProviderFactories_GetFactory()
		{
			var cs = ConfigurationManager.ConnectionStrings["MyConnectionString"];
			var providerName = cs.ProviderName;

			var table = DbProviderFactories.GetFactoryClasses(); // Name, Description, InvariantName, AssemblyQualifiedName
			Assert.IsNotNull (table);

			var Rows = table.Select (string.Format("InvariantName = '{0}'", providerName));
			Assert.AreEqual (Rows.Length, 1);

			var assemblyName = Rows [0] ["AssemblyQualifiedName"];
			Assert.IsNotNullOrEmpty (assemblyName as string);

			// see http://www.mono-project.com/docs/advanced/pinvoke/dllnotfoundexception/
			// on how to view load info with MONO_LOG_LEVEL=debug /usr/bin/mono ...
			var a = Assembly.Load ((string)assemblyName);
			Assert.IsNotNull (a);

			var factory = DbProviderFactories.GetFactory(providerName); 			
			Assert.IsNotNull (factory);
		}

		[Test]
		public void Test_RunSimpleDbCommand()
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
