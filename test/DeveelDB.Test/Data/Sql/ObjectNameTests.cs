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

namespace Deveel.Data.Sql {
	public class ObjectNameTests {
		[Fact]
		public void CreateSimpleName() {
			var name = new ObjectName("a");

			Assert.Equal("a", name.FullName);
			Assert.Null(name.Parent);
			Assert.Null(name.ParentName);
			Assert.False(name.IsGlob);
		}

		[Fact]
		public void ParseSimpleName() {
			var name = ObjectName.Parse("a");

			Assert.Equal("a", name.FullName);
			Assert.Null(name.Parent);
			Assert.Null(name.ParentName);
			Assert.False(name.IsGlob);
		}

		[Theory]
		[InlineData("a.")]
		[InlineData(".b")]
		[InlineData("a b")]
		[InlineData("a.[b")]
		public void ParseInvalidName(string s) {
			Assert.Throws<FormatException>(() => ObjectName.Parse(s));
		}

		[Fact]
		public void ParseEmptyName() {
			Assert.Throws<ArgumentNullException>(() => ObjectName.Parse(""));
		}

		[Fact]
		public void ParseComposedName() {
			ObjectName name;
			Assert.True(ObjectName.TryParse("a.b", out name));

			Assert.Equal("a.b", name.FullName);
			Assert.NotNull(name.Parent);
			Assert.NotNull(name.ParentName);
			Assert.Equal("b", name.Name);
			Assert.Equal("a", name.ParentName);
			Assert.False(name.IsGlob);
		}

		[Fact]
		public void ParseGlobName() {
			ObjectName name;
			Assert.True(ObjectName.TryParse("a.*", out name));

			Assert.Equal("a.*", name.FullName);
			Assert.NotNull(name.Parent);
			Assert.NotNull(name.ParentName);
			Assert.Equal("*", name.Name);
			Assert.Equal("a", name.ParentName);
			Assert.True(name.IsGlob);
		}

		[Fact]
		public void GetSqlFromName() {
			ObjectName name;
			Assert.True(ObjectName.TryParse("a.*", out name));

			Assert.Equal("a.*", name.ToString());
		}

		[Theory]
		[InlineData("a", "a", 0)]
		[InlineData("a", "b", -1)]
		[InlineData("name.1", "name.1", 0)]
		[InlineData("name.1", "name.2", -1)]
		[InlineData("a.*", "a.*", 0)]
		[InlineData("a.*", "b.*", -1)]
		public void CompareTwoNames(string s1, string s2, int expected) {
			var name1 = ObjectName.Parse(s1);
			var name2 = ObjectName.Parse(s2);

			Assert.Equal(expected, name1.CompareTo(name2));
		}

		[Theory]
		[InlineData("a", "c", false)]
		[InlineData("foo", "bar", false)]
		[InlineData("tab1", "tab1", true)]
		[InlineData("a.1", "a.2", false)]
		[InlineData("a.*", "b.*", false)]
		[InlineData("a.*", "a.*", true)]
		public void TwoNamesEqual(string s1, string s2, bool expected) {
			var name1 = ObjectName.Parse(s1);
			var name2 = ObjectName.Parse(s2);

			Assert.Equal(expected, name1.Equals(name2));
		}

		[Theory]
		[InlineData("a", "c", false, false)]
		[InlineData("foo", "Bar", true, false)]
		[InlineData("tab1", "TAB1", true, true)]
		[InlineData("a.*", "B.*", true, false)]
		[InlineData("a.*", "A.*", true, true)]
		public void TwoNamesCaseEqual(string s1, string s2, bool ignoreCase, bool expected) {
			var name1 = ObjectName.Parse(s1);
			var name2 = ObjectName.Parse(s2);

			Assert.Equal(expected, name1.Equals(name2, ignoreCase));
		}


		[Theory]
		[InlineData("a", "b", "a.b")]
		[InlineData("a.b", "c", "a.b.c")]
		[InlineData("a.b", "*", "a.b.*")]
		[InlineData("a", "b.c", "a.b.c")]
		public void AppendComplexName(string s, string childName, string expected) {
			var name = ObjectName.Parse(s);
			var name1 = ObjectName.Parse(childName);

			var child = name.Append(name1);

			Assert.NotNull(child);
			Assert.Equal(expected, child.FullName);
		}

		[Theory]
		[InlineData("a", "b", "a.b")]
		[InlineData("a.b", "c", "a.b.c")]
		[InlineData("a.b", "*", "a.b.*")]
		public void AppendSimpleName(string s, string childName, string expected) {
			var name = ObjectName.Parse(s);

			var child = name.Append(childName);

			Assert.NotNull(child);
			Assert.Equal(expected, child.FullName);
		}

		[Theory]
		[InlineData("a", "b", false, -1)]
		[InlineData("ab.cd", "aB.CD", true, 0)]
		[InlineData("aa.BB", "aa.Bb", false, -32)]
		public void CompareOrdinal(string s1, string s2, bool ignoreCase, int expected) {
			var comparer = new ObjectNameComparer(ignoreCase);
			var name1 = ObjectName.Parse(s1);
			var name2 = ObjectName.Parse(s2);

			var result = comparer.Compare(name1, name2);

			Assert.Equal(expected, result);
		}

		//[Theory]
		//[InlineData("a.b")]
		//[InlineData("a")]
		//[InlineData("a.b.c")]
		//public void Serialize(string name) {
		//	var objName = ObjectName.Parse(name);
		//	var result = BinarySerializeUtil.Serialize(objName);

		//	Assert.Equal(objName, result);
		//}
	}
}