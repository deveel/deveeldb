using System;

using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public class SystemFunctionTests : ContextBasedTest {
		protected override IDatabaseContext CreateDatabaseContext(ISystemContext context) {
			var dbContext = base.CreateDatabaseContext(context);
			dbContext.UseSystemFunctions();
			return dbContext;
		}

		[Test]
		public void ResolveSystemFunctionWithNoSchema() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = QueryContext.ResolveFunction(new ObjectName("user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void ResolveSystemFunctionFullyQualified() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = QueryContext.ResolveFunction(ObjectName.Parse("SYSTEM.user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void ExecuteUserFunction() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = QueryContext.ResolveFunction(new ObjectName("user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);

			ExecuteResult result=null;
			Assert.DoesNotThrow(() => result = function.Execute(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(AdminUserName, result.ReturnValue.AsVarChar().Value.ToString());
		}
	}
}
