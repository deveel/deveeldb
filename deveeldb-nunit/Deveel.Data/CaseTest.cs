using System;
using System.Collections.Generic;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CaseTest : TestBase {
		[Test]
		public void StandaloneTest() {
			TObject result = Expression.Evaluate("CASE " +
			                                     "  WHEN true THEN 1" +
			                                     "  ELSE 0");
			Assert.IsTrue(result == (TObject) 1);
		}
		
		[Test]
		public void VariableTest() {
			Dictionary<string, object> vars = new Dictionary<string, object>();
			vars["var1"] = true;
			vars["var2"] = false;
			vars["var3"] = 22;
			vars["var4"] = 13;
			
			string expression = "CASE var3 WHEN var1 ELSE var4" ;
			TObject result = Expression.Evaluate(expression, vars);
			
			Assert.IsTrue(result == (TObject)22);
		}
	}
}