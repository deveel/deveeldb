using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateProcedureTests : ContextBasedTest {
		[Test]
		public void CreateExternalProcedure() {
			var procName = ObjectName.Parse("APP.proc1");
			var parameters = new RoutineParameter[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()),
				new RoutineParameter("b", PrimitiveTypes.Integer()),
			};

			var externRef = ExternalRef.MakeRef(typeof(Test), "Procedure(int, int)");
			Query.CreateExternProcedure(procName, parameters, externRef.ToString());

			var exists = Query.Access().RoutineExists(procName);

			Assert.IsTrue(exists);

			var procedure = Query.Access().GetObject(DbObjectType.Routine, procName);

			Assert.IsNotNull(procedure);
			Assert.IsInstanceOf<ExternalProcedure>(procedure);

			var externFunction = (ExternalProcedure)procedure;
			Assert.IsNotNull(externFunction.ExternalRef);
			Assert.AreEqual(typeof(Test), externFunction.ExternalRef.Type);

		}

		static class Test {
			public static int Result;

			public static void Procedure(int a, int b) {
				Result = a + b;
			}
		}
	}
}
