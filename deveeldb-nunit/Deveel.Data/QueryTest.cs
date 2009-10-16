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

		[Test]
		public void AvgAge() {
			Console.Out.WriteLine("Computing the average ages of people in 'Person' table...");

			IDbConnection connection = CreateConnection();

			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT AVG(age) FROM Person";
			IDataReader reader = command.ExecuteReader();

			if (reader.Read())
				Console.Out.WriteLine("Average age of people: {0}", reader.GetDouble(0));

			reader.Close();
		}

		[Test]
		public void PeopleInAfrica() {
			Console.Out.WriteLine("Selecting all the people in 'Person' table who live in Africa...");

			IDbConnection connection = CreateConnection();

			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT name FROM Person WHERE lives_in = 'Africa' ORDER BY name";

			IDataReader reader = command.ExecuteReader();
			Console.Out.WriteLine("All people that live in Africa:");
			while (reader.Read()) {
				Console.Out.WriteLine("  " + reader.GetString(0));
			}
			Console.Out.WriteLine();
		}

		[Test]
		public void OasisOrBeatles() {
			// List the name and music group of all the people that listen to
			// either 'Oasis' or 'Beatles'
			IDbConnection connection = CreateConnection();
			IDbCommand command = connection.CreateCommand();
			command.CommandText = "   SELECT Person.name, MusicGroup.name " +
								  "     FROM Person, ListensTo, MusicGroup " +
								  "    WHERE MusicGroup.name IN ( 'Oasis', 'Beatles' ) " +
								  "      AND Person.name = ListensTo.person_name " +
								  "      AND ListensTo.music_group_name = MusicGroup.name " +
								  " ORDER BY MusicGroup.name, Person.name ";
			IDataReader result = command.ExecuteReader();
			Console.Out.WriteLine("All people that listen to either Beatles or Oasis:");
			while (result.Read()) {
				Console.Out.Write("  " + result.GetString(0));
				Console.Out.Write(" listens to ");
				Console.Out.WriteLine(result.GetString(1));
			}
			Console.Out.WriteLine();
		}
	}
}