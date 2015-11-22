using System;

using Deveel.Data.Mapping;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public class ContextQueryTests : ContextBasedTest {
		[Test]
		public void CreateContext() {
			QueryContext context = null;
			Assert.DoesNotThrow(() => context =  new EmptyTestQueryContext(Query));
			Assert.IsNotNull(context);
		}

		[Test]
		public void RequestTableNotConfigured() {
			var context = new EmptyTestQueryContext(Query);
			Assert.DoesNotThrow(() => context.Table<EmptyTestType>());
		}

		[Test]
		public void RequestEntryNotConfigured() {
			var context = new EmptyTestQueryContext(Query);
			Assert.Throws<QueryException>(() => context.Table<EmptyTestType>().FindById(1));
		}

		class EmptyTestType { 
		}

		class EmptyTestQueryContext : QueryContext {
			public EmptyTestQueryContext(IQuery context)
				: base(context) {
			}

			protected override void OnBuildMap(MappingContext mappingContext) {
				
			}
		}
	}
}
