using System;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Xml {
	[TestFixture]
	public class XmlFunctionsTest : ContextBasedTest {
		protected override void RegisterServices(ServiceContainer container) {
			container.UseXml();
		}

		private Field ParseAndInvoke(string text) {
			var exp = SqlExpression.Parse(text);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(exp);

			var functionName = ((SqlFunctionCallExpression)exp).FunctioName;
			var args = ((SqlFunctionCallExpression)exp).Arguments;
			var invoke = new Invoke(functionName, args);

			return Query.InvokeFunction(invoke);
		}

		[Test]
		public void ToXmlType() {
			const string text = "TO_XML('<root>value</root>')";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<XmlNodeType>(result.Type);
			Assert.IsFalse(result.IsNull);
		}

		[Test]
		public void ExtractXml() {
			const string text = "EXTRACT(TO_XML('<root><child>value</child></root>'), '/root/child')";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<XmlNodeType>(result.Type);
			Assert.IsFalse(result.IsNull);
		}

		[Test]
		public void ExtractValue() {
			const string text = "EXTRACTVALUE(TO_XML('<root><child>value</child></root>'), '/root/child/text()')";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);
			Assert.IsFalse(result.IsNull);
		}

		[Test]
		public void UpdateText() {
			const string text = "UPDATEXML(TO_XML('<root><child>value</child></root>'), '/root/child/text()', 'value2')";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<XmlNodeType>(result.Type);
			Assert.IsFalse(result.IsNull);

			/*
			TODO: must inspect why the text fails to compare...
			var xmlNode = (SqlXmlNode) result.Value;
			var updated = xmlNode.ToSqlString().Value;
			var expected = "<root><child>value2</child></root>";
			Assert.AreEqual(expected, updated);
			*/
		}
	}
}
