// 
//  Copyright 2010-2014 Deveel
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

using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[Category("Object Name")]
	public class ObjectNameTest {
		[Test]
		public void SimpleName() {
			var objName = new ObjectName("id");
			Assert.IsNotNull(objName);
			Assert.IsNull(objName.Parent);
			Assert.AreEqual("id", objName.Name);
			Assert.AreEqual("id", objName.FullName);
			Assert.IsFalse(objName.IsGlob);
		}

		[Test]
		public void ComplexName() {
			var objName = new ObjectName(new ObjectName("parent"), "id");
			Assert.IsNotNull(objName);
			Assert.IsNotNull(objName.Parent);
			Assert.AreEqual("parent", objName.Parent.Name);
			Assert.IsNotNull(objName.Name);
			Assert.AreEqual("id", objName.Name);
			Assert.AreEqual("parent.id", objName.FullName);
			Assert.IsFalse(objName.IsGlob);
		}

		[Test]
		public void ComplexNameWithGlob() {
			var objName = new ObjectName(new ObjectName("parent"), "*");
			Assert.IsNotNull(objName);
			Assert.IsNotNull(objName.Parent);
			Assert.AreEqual("parent", objName.Parent.Name);
			Assert.IsNotNull(objName.Name);
			Assert.AreEqual("*", objName.Name);
			Assert.AreEqual("parent.*", objName.FullName);
			Assert.IsTrue(objName.IsGlob);
		}

		[Test]
		public void SimpleName_Parse() {
			ObjectName objName = null;
			Assert.DoesNotThrow(() => objName = ObjectName.Parse("id"));
			Assert.IsNotNull(objName);
			Assert.AreEqual("id", objName.Name);
			Assert.AreEqual("id", objName.FullName);
			Assert.IsFalse(objName.IsGlob);
		}

		[Test]
		public void ComplexName_Parse() {
			ObjectName objName = null;
			Assert.DoesNotThrow(() => objName = ObjectName.Parse("parent.id"));
			Assert.IsNotNull(objName);
			Assert.IsNotNull(objName.Parent);
			Assert.AreEqual("parent", objName.Parent.Name);
			Assert.IsNotNull(objName.Name);
			Assert.AreEqual("id", objName.Name);
			Assert.AreEqual("parent.id", objName.FullName);
			Assert.IsFalse(objName.IsGlob);
		}

		[Test]
		public void SimpleName_Compare() {
			var objName1 = new ObjectName("id1");
			var objName2 = new ObjectName("id2");

			int i = -2;
			Assert.DoesNotThrow(() => i = objName1.CompareTo(objName2));
			Assert.AreEqual(-1, i);
		}

		[Test]
		public void SimpleName_Equals() {
			var objName1 = new ObjectName("id1");
			var objName2 = new ObjectName("id1");
			Assert.IsTrue(objName1.Equals(objName2));
		}

		[Test]
		public void SimpleName_NotEquals() {
			var objName1 = new ObjectName("id1");
			var objName2 = new ObjectName("id2");
			Assert.IsFalse(objName1.Equals(objName2));
		}
	}
}
