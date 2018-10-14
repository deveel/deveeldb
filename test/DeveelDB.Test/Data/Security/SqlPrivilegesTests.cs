// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

using Xunit;

namespace Deveel.Data.Security {
	public static class SqlPrivilegesTests {
		[Theory]
		[InlineData("INSERT")]
		[InlineData("Insert")]
		[InlineData("SELECT")]
		[InlineData("Update")]
		[InlineData("usage")]
		[InlineData("delete")]
		[InlineData("References")]
		[InlineData("List")]
		[InlineData("Execute")]
		[InlineData("Drop")]
		[InlineData("Create")]
		public static void ParsePrivilege(string s) {
			var privilege = SqlPrivileges.Resolver.ResolvePrivilege(s);
			Assert.NotEqual(Privilege.None, privilege);
		}

		[Fact]
		public static void FormatToString() {
			Assert.Equal("INSERT", SqlPrivileges.Insert.ToString());
			Assert.Equal("DELETE", SqlPrivileges.Delete.ToString());
			Assert.Equal("UPDATE", SqlPrivileges.Update.ToString());
			Assert.Equal("USAGE", SqlPrivileges.Usage.ToString());
			Assert.Equal("EXECUTE", SqlPrivileges.Execute.ToString());
			Assert.Equal("LIST", SqlPrivileges.List.ToString());
			Assert.Equal("REFERENCES", SqlPrivileges.References.ToString());
			Assert.Equal("SELECT, INSERT", (SqlPrivileges.Select + SqlPrivileges.Insert).ToString());
			Assert.Equal("DROP, ALTER, CREATE", (SqlPrivileges.Create + SqlPrivileges.Alter + SqlPrivileges.Drop).ToString());
		}

		[Fact]
		public static void RemoveFromPrivilege() {
			var privileges = SqlPrivileges.Insert + SqlPrivileges.Delete;
			
			Assert.True(privileges.Permits(SqlPrivileges.Insert));

			var result = privileges - SqlPrivileges.Insert;

			Assert.Equal(SqlPrivileges.Delete, result);
		}

		[Fact]
		public static void RemoveNotDefinedPrivilege() {
			var privileges = SqlPrivileges.Insert + SqlPrivileges.Delete;
			var result = privileges - SqlPrivileges.Update;

			Assert.Equal(privileges, result);
		}
	}
}