// 
//  QueryTest.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;

using NUnit.Framework;


namespace Deveel.Data {
	[TestFixture()]
	public class QueryTest : TestBase {
		[Test()]
		public void CountPeople() {
			Console.Out.WriteLine("Counting the number of rows in 'Person' table...");
			
			IDbConnection connection = CreateConnection();
			// Create a Statement object to execute the queries on,
			IDbCommand statement = connection.CreateCommand();
			IDataReader result;
			
			// How many rows are in the 'Person' table?
			statement.CommandText = "SELECT COUNT(*) FROM Person";
			result = statement.ExecuteReader();
			if (result.Read()) {
				Console.Out.WriteLine("Rows in 'Person' table: " + result.GetInt32(0));
			}
		}
	}
}
