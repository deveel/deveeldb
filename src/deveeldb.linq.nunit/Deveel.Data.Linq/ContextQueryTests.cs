using System;

using Deveel.Data.Mapping;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public class ContextQueryTests : ContextBasedTest {
		[Test]
		public void CreateContext() {
			var settings = new QueryContextSettings {
				UserName = AdminUserName,
				Password = AdminPassword
			};

			QueryContext context = null;
			Assert.DoesNotThrow(() => context =  new EmptyTestQueryContext(Database, settings));
			Assert.IsNotNull(context);
		}

		[Test]
		public void RequestTableNotConfigured() {
			var settings = new QueryContextSettings {
				UserName = AdminUserName,
				Password = AdminPassword
			};

			var context = new EmptyTestQueryContext(Database, settings);
			Assert.DoesNotThrow(() => context.Table<EmptyTestType>());
		}

		[Test]
		public void RequestEntryNotConfigured() {
			var settings = new QueryContextSettings {
				UserName = AdminUserName,
				Password = AdminPassword
			};

			var context = new EmptyTestQueryContext(Database, settings);
			Assert.Throws<QueryException>(() => context.Table<EmptyTestType>().FindById(1));
		}

		class EmptyTestType { 
		}

		class EmptyTestQueryContext : QueryContext {
			public EmptyTestQueryContext(IDatabase database, QueryContextSettings settings)
				: base(database, settings) {
			}

			protected override void OnBuildMap(MappingContext mappingContext) {
				
			}
		}
	}
}
