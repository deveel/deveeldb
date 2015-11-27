using System;
using System.IO;

using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	[TestFixture]
	public static class BinarySerializeTests {
		[Test]
		public static void SerializeWithNoParent() {
			var obj = new TestClass {Value = "test1"};

			var serializer = new BinarySerializer();
			byte[] bytes;

			using (var memoryStream = new MemoryStream()) {
				serializer.Serialize(memoryStream, obj);
				memoryStream.Flush();
				bytes = memoryStream.ToArray();
			}

			object graph = null;
			using (var memoryStream = new MemoryStream(bytes)) {
				graph = serializer.Deserialize(memoryStream, typeof(TestClass));
			}

			Assert.IsNotNull(graph);
			Assert.IsInstanceOf<TestClass>(obj);

			obj = (TestClass) graph;
			Assert.AreEqual("test1", obj.Value);
			Assert.IsNull(obj.Parent);
		}

		[Test]
		public static void SerializeObjectNameWithNoParent() {
			var objName = new ObjectName("name");

			var serializer = new BinarySerializer();
			byte[] bytes;

			using (var memoryStream = new MemoryStream()) {
				serializer.Serialize(memoryStream, objName);
				memoryStream.Flush();
				bytes = memoryStream.ToArray();
			}

			object graph = null;
			using (var memoryStream = new MemoryStream(bytes)) {
				graph = serializer.Deserialize(memoryStream, typeof(ObjectName));
			}

			Assert.IsNotNull(graph);
			Assert.IsInstanceOf<ObjectName>(graph);

			var objName2 = (ObjectName) graph;
			Assert.AreEqual(objName.Name, objName2.Name);
			Assert.AreEqual(objName, objName2);
		}

		[Test]
		public static void SerializeImplicit() {
			var obj = new TestClass2("test2");

			var serializer = new BinarySerializer();
			byte[] bytes;

			using (var memoryStream = new MemoryStream()) {
				serializer.Serialize(memoryStream, obj);
				memoryStream.Flush();
				bytes = memoryStream.ToArray();
			}

			object graph = null;
			using (var memoryStream = new MemoryStream(bytes)) {
				graph = serializer.Deserialize(memoryStream, typeof(TestClass2));
			}

			Assert.IsNotNull(graph);
			Assert.IsInstanceOf<TestClass2>(obj);

			obj = (TestClass2) graph;
			Assert.AreEqual("test2", obj.Value);
			Assert.IsNull(obj.Parent);

		}

		[Serializable]
		class TestClass : ISerializable {
			public TestClass() {	
			}

			private TestClass(ObjectData graph) {
				Value = graph.GetString("value");
				Parent = graph.GetValue<TestClass>("parent");
			}

			public string Value { get; set; }

			public TestClass Parent { get; set; }

			public void GetData(SerializeData graph) {
				graph.SetValue("value", Value);
				graph.SetValue("parent", Parent);
			}
		}

		[Serializable]
		class TestClass2 {
			[NonSerialized]
			internal TestClass2 parent;

			private TestClass2() {
			}

			public TestClass2(string value) {
				Value = value;
			}

			public string Value { get; private set; }

			public TestClass2 Parent {
				get { return parent; }
				set {
					parent = value;
				}
			}
		}
	}
}
