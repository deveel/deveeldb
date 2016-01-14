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
	public partial class Test
	{

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
				cmd.CommandText = "INSERT INTO mytable (myident, mystamp, mycontent) VALUES (1, TODATE('2016-01-01'), '2 weeks ago'), (2, TODATE('2016-01-07'), '1 week ago');";
				int affected = cmd.ExecuteNonQuery ();
				Assert.AreEqual (2, affected);
			}
		}

		static void Insert_WithToDateTime (IDbConnection con, IDbTransaction tran)
		{
			using (var cmd = con.CreateCommand ()) {
				Assert.IsNotNull (cmd);
				cmd.CommandText = "INSERT INTO mytable (myident, mystamp, mycontent) VALUES (1, todatetime('2016-01-01'), '2 weeks ago'), (2, todatetime('2016-01-07'), '1 week ago');";
				int affected = cmd.ExecuteNonQuery ();
				Assert.AreEqual (2, affected);
			}
		}

		static void InsertData2 (IDbConnection con, IDbTransaction tran)
		{
			using (var cmd = con.CreateCommand ()) {
				Assert.IsNotNull (cmd);
				cmd.CommandText = "INSERT INTO mytable (myident, mystamp, mycontent) VALUES (1, CAST('2016-01-01' AS DATE), '2 weeks ago'), (2, CAST('2016-01-07' as DATE), '1 week ago');";
				int affected = cmd.ExecuteNonQuery ();
				Assert.AreEqual (2, affected);
			}
		}
	}
}

